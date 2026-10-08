# Databricks notebook source
# ---- Silver merge: apply Bronze events to Silver current-state table ----
# See design doc: "Applying changes in Databricks" section.
#
# Operation | Effect on current-state table
# I         | Insert the new row
# U         | Replace the row identified by the primary key with new_row
# D         | Delete the row identified by the primary key

from pyspark.sql.functions import from_json, col, get_json_object, row_number
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType, TimestampType
from pyspark.sql.window import Window

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

orders_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("total", DecimalType(12, 2), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
])

# 1. Read the last silver checkpoint
silver_watermark = spark.sql(f"""
  SELECT COALESCE(max(last_event_id), 0) AS last_event_id
  FROM {catalog}.{schema}.silver_checkpoint
  WHERE source_table = 'orders'
""").collect()[0]["last_event_id"]
print(f"Silver merge starting from event_id > {silver_watermark}")

# 2. Read new Bronze events (deduplicated by event_id — at-least-once delivery)
new_events = spark.sql(f"""
  SELECT
    event_id,
    operation,
    primary_key,
    new_row,
    old_row
  FROM (
    SELECT
      event_id,
      operation,
      primary_key,
      new_row,
      old_row,
      ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) AS rn
    FROM {catalog}.{schema}.bronze_change_log
    WHERE event_id > {silver_watermark}
      AND source_table = 'orders'
  )
  WHERE rn = 1
  ORDER BY event_id
""").cache()

event_count = new_events.count()
print(f"Found {event_count} new events to apply to Silver")

if event_count == 0:
    print("No new events — Silver is up to date.")
    dbutils.notebook.exit("up_to_date")

# 3. Deduplicate by order_id keeping the latest event for each key.
# This correctly handles interleaved operations (e.g. INSERT→DELETE→INSERT)
# because the final operation for each PK determines the end state.
latest_per_key = (
    new_events
    .withColumn(
        "rn",
        row_number().over(
            Window.partitionBy(
                get_json_object(col("primary_key"), "$.order_id")
            ).orderBy(col("event_id").desc())
        )
    )
    .filter("rn = 1")
    .drop("rn")
)

# 4. Apply upserts (operation = 'I' or 'U') — insert or update the row
upserts = latest_per_key.filter("operation IN ('I', 'U')")

if upserts.count() > 0:
    parsed_upserts = upserts.select(
        from_json(col("new_row"), orders_schema).alias("row")
    ).select(
        col("row.order_id"),
        col("row.customer_id"),
        col("row.status"),
        col("row.total"),
        col("row.created_at"),
        col("row.updated_at"),
    )

    parsed_upserts.createOrReplaceTempView("upserts_view")
    spark.sql(f"""
      MERGE INTO {catalog}.{schema}.silver_orders AS t
      USING upserts_view AS s
      ON t.order_id = s.order_id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
    print("Applied upserts (INSERT/UPDATE) to Silver")

# 5. Apply DELETEs (operation = 'D')
deletes = latest_per_key.filter("operation = 'D'")
delete_count = deletes.count()

if delete_count > 0:
    delete_ids = deletes.select(
        get_json_object(col("primary_key"), "$.order_id").cast("long").alias("order_id"),
    )

    delete_ids.createOrReplaceTempView("deletes_view")
    spark.sql(f"""
      MERGE INTO {catalog}.{schema}.silver_orders AS t
      USING deletes_view AS s
      ON t.order_id = s.order_id
      WHEN MATCHED THEN DELETE
    """)
    print(f"Applied {delete_count} DELETEs to Silver")

# 6. Advance the silver checkpoint
new_silver_watermark = new_events.agg({"event_id": "max"}).collect()[0][0]

spark.sql(f"""
  MERGE INTO {catalog}.{schema}.silver_checkpoint AS t
  USING (
    SELECT 'orders' AS source_table, {new_silver_watermark} AS last_event_id, current_timestamp() AS updated_at
  ) AS s
  ON t.source_table = s.source_table
  WHEN MATCHED THEN UPDATE SET last_event_id = s.last_event_id, updated_at = s.updated_at
  WHEN NOT MATCHED THEN INSERT (source_table, last_event_id, updated_at) VALUES (s.source_table, s.last_event_id, s.updated_at)
""")

new_events.unpersist()

print(f"Silver checkpoint advanced to {new_silver_watermark}")
print(f"Silver merge complete: {event_count} events applied.")

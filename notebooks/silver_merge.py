# Databricks notebook source
# ---- Silver merge: apply Bronze events to Silver current-state table ----
# See design doc: "Applying changes in Databricks" section.
#
# Operation | Effect on current-state table
# I         | Insert the new row
# U         | Replace the row identified by the primary key with new_row
# D         | Delete the row identified by the primary key

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

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
""")

event_count = new_events.count()
print(f"Found {event_count} new events to apply to Silver")

if event_count == 0:
    print("No new events — Silver is up to date.")
    dbutils.notebook.exit("up_to_date")

# 3. Apply INSERTs (operation = 'I') — dedup by order_id, keep latest event
inserts = new_events.filter("operation = 'I'").select(
    "event_id",
    "primary_key",
    "new_row"
)

if inserts.count() > 0:
    from pyspark.sql.functions import from_json, col, get_json_object, row_number
    from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType, TimestampType
    from pyspark.sql.window import Window

    orders_schema = StructType([
        StructField("order_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("status", StringType(), True),
        StructField("total", DecimalType(12, 2), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])

    parsed_inserts = inserts.select(
        col("event_id"),
        from_json(col("new_row"), orders_schema).alias("row")
    ).select(
        col("event_id"),
        col("row.order_id"),
        col("row.customer_id"),
        col("row.status"),
        col("row.total"),
        col("row.created_at"),
        col("row.updated_at"),
    )

    deduped_inserts = parsed_inserts.withColumn(
        "rn", row_number().over(Window.partitionBy("order_id").orderBy(col("event_id").desc()))
    ).filter("rn = 1").drop("rn", "event_id")

    deduped_inserts.createOrReplaceTempView("inserts_view")
    spark.sql(f"""
      MERGE INTO {catalog}.{schema}.silver_orders AS t
      USING inserts_view AS s
      ON t.order_id = s.order_id
      WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Applied INSERTs to Silver")

# 4. Apply UPDATEs (operation = 'U') — dedup by order_id, keep latest event
updates = new_events.filter("operation = 'U'").select(
    "event_id",
    "primary_key",
    "new_row"
)

if updates.count() > 0:
    from pyspark.sql.functions import from_json, col, row_number
    from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType, TimestampType
    from pyspark.sql.window import Window

    orders_schema = StructType([
        StructField("order_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("status", StringType(), True),
        StructField("total", DecimalType(12, 2), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])

    parsed_updates = updates.select(
        col("event_id"),
        from_json(col("new_row"), orders_schema).alias("row")
    ).select(
        col("event_id"),
        col("row.order_id"),
        col("row.customer_id"),
        col("row.status"),
        col("row.total"),
        col("row.created_at"),
        col("row.updated_at"),
    )

    deduped_updates = parsed_updates.withColumn(
        "rn", row_number().over(Window.partitionBy("order_id").orderBy(col("event_id").desc()))
    ).filter("rn = 1").drop("rn", "event_id")

    deduped_updates.createOrReplaceTempView("updates_view")
    spark.sql(f"""
      MERGE INTO {catalog}.{schema}.silver_orders AS t
      USING updates_view AS s
      ON t.order_id = s.order_id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Applied UPDATEs to Silver")

# 5. Apply DELETEs (operation = 'D') — dedup by order_id, keep latest event
deletes = new_events.filter("operation = 'D'")
delete_count = deletes.count()

if delete_count > 0:
    from pyspark.sql.functions import get_json_object, col, row_number
    from pyspark.sql.window import Window

    delete_ids = deletes.select(
        col("event_id"),
        get_json_object(col("primary_key"), "$.order_id").cast("long").alias("order_id"),
    )

    deduped_deletes = delete_ids.withColumn(
        "rn", row_number().over(Window.partitionBy("order_id").orderBy(col("event_id").desc()))
    ).filter("rn = 1").drop("rn", "event_id")

    deduped_deletes.createOrReplaceTempView("deletes_view")
    spark.sql(f"""
      MERGE INTO {catalog}.{schema}.silver_orders AS t
      USING deletes_view AS s
      ON t.order_id = s.order_id
      WHEN MATCHED THEN DELETE
    """)
    print(f"Applied DELETEs to Silver")

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

print(f"Silver checkpoint advanced to {new_silver_watermark}")
print(f"Silver merge complete: {event_count} events applied.")

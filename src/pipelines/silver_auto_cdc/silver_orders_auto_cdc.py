# ---- Silver via Auto CDC: alternative to notebooks/silver_merge.py ----
# Builds silver_orders_auto_cdc (SCD Type 1) from the same Bronze change log,
# letting the pipeline handle ordering, dedup-by-key, and deletes.

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, from_json, get_json_object, lit

BRONZE_TABLE = spark.conf.get("bronze_table")
SNAPSHOT_TABLE = spark.conf.get("snapshot_table")
TARGET = "silver_orders_auto_cdc"

ORDERS_SCHEMA = (
    "order_id BIGINT, customer_id BIGINT, status STRING, total DECIMAL(12,2), "
    "created_at TIMESTAMP, updated_at TIMESTAMP"
)
ORDER_COLUMNS = ["customer_id", "status", "total", "created_at", "updated_at"]


@dp.temporary_view()
def orders_snapshot_events():
    return (
        spark.readStream.table(SNAPSHOT_TABLE)
        .select(
            "order_id",
            *ORDER_COLUMNS,
            col("snapshot_event_id").alias("event_id"),
            lit("I").alias("operation"),
        )
    )


# Replayed Bronze duplicates share key, event_id and payload, so Auto CDC resolves them identically.
@dp.temporary_view()
def orders_change_events():
    return (
        spark.readStream.table(BRONZE_TABLE)
        .where("source_table = 'orders'")
        .withColumn("row", from_json(col("new_row"), ORDERS_SCHEMA))
        .select(
            get_json_object(col("primary_key"), "$.order_id").cast("bigint").alias("order_id"),
            *[col(f"row.{c}").alias(c) for c in ORDER_COLUMNS],
            "event_id",
            "operation",
        )
    )


dp.create_streaming_table(name=TARGET)

dp.create_auto_cdc_flow(
    name="orders_initial_snapshot",
    target=TARGET,
    source="orders_snapshot_events",
    keys=["order_id"],
    sequence_by="event_id",
    except_column_list=["event_id", "operation"],
    stored_as_scd_type=1,
    once=True,
)

dp.create_auto_cdc_flow(
    name="orders_change_log",
    target=TARGET,
    source="orders_change_events",
    keys=["order_id"],
    sequence_by="event_id",
    apply_as_deletes=expr("operation = 'D'"),
    except_column_list=["event_id", "operation"],
    stored_as_scd_type=1,
)

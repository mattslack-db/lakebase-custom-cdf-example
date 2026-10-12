# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade
# COMMAND ----------
# MAGIC dbutils.library.restartPython()
# COMMAND ----------
# ---- Initial snapshot: copy app.orders into Silver, record starting watermark ----
# See design doc: "Initial load and cutover" section.

from databricks.sdk import WorkspaceClient

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
lakebase_project = dbutils.widgets.get("lakebase_project")
lakebase_branch = dbutils.widgets.get("lakebase_branch")
lakebase_endpoint = dbutils.widgets.get("lakebase_endpoint")
lakebase_database = dbutils.widgets.get("lakebase_database")
lakebase_role = dbutils.widgets.get("lakebase_role")

endpoint_name = f"projects/{lakebase_project}/branches/{lakebase_branch}/endpoints/{lakebase_endpoint}"

w = WorkspaceClient()
ep = w.postgres.get_endpoint(name=endpoint_name)
pg_host = ep.status.hosts.host
cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
pg_token = cred.token

snapshot_table = f"{catalog}.{schema}.orders_snapshot"
has_snapshot = spark.catalog.tableExists(snapshot_table)
silver_count = spark.sql(f"SELECT count(*) FROM {catalog}.{schema}.silver_orders").collect()[0][0]
if has_snapshot and silver_count > 0:
    print(f"Silver has {silver_count} rows and {snapshot_table} exists — skipping initial snapshot.")
    dbutils.notebook.exit("skipped")

# 1. Record the initial event watermark (read before the snapshot so every event <= watermark is reflected in it)
jdbc_url = f"jdbc:postgresql://{pg_host}:5432/{lakebase_database}"
watermark_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "(SELECT COALESCE(max(event_id), 0) AS max_event_id FROM cdc.change_log) AS t")
    .option("user", lakebase_role)
    .option("password", pg_token)
    .option("sslmode", "require")
    .load()
)
initial_watermark = watermark_df.collect()[0][0]
print(f"Initial watermark (max event_id in change_log): {initial_watermark}")

# 2. Snapshot app.orders (cached so both writes see the same rows)
orders_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "app.orders")
    .option("user", lakebase_role)
    .option("password", pg_token)
    .option("sslmode", "require")
    .load()
).cache()
row_count = orders_df.count()

# Immutable snapshot used as the starting state for the Auto CDC pipeline
if not has_snapshot:
    from pyspark.sql.functions import lit
    (
        orders_df
        .withColumn("snapshot_event_id", lit(initial_watermark).cast("bigint"))
        .write.format("delta").mode("overwrite")
        .saveAsTable(snapshot_table)
    )
    print(f"Wrote {row_count} rows to {snapshot_table} at watermark {initial_watermark}")

if silver_count > 0:
    orders_df.unpersist()
    print(f"Silver already has {silver_count} rows — leaving Silver and checkpoints unchanged.")
    dbutils.notebook.exit("snapshot_only")

print(f"Snapshotting {row_count} rows from app.orders into Silver...")
(
    orders_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.silver_orders")
)
orders_df.unpersist()

# 3. Record the initial watermark in both checkpoints
spark.sql(f"""
  MERGE INTO {catalog}.{schema}.ingestion_checkpoint AS t
  USING (
    SELECT 'orders' AS source_table, {initial_watermark} AS last_event_id, current_timestamp() AS updated_at
  ) AS s
  ON t.source_table = s.source_table
  WHEN MATCHED THEN UPDATE SET last_event_id = s.last_event_id, updated_at = s.updated_at
  WHEN NOT MATCHED THEN INSERT (source_table, last_event_id, updated_at) VALUES (s.source_table, s.last_event_id, s.updated_at)
""")

spark.sql(f"""
  MERGE INTO {catalog}.{schema}.silver_checkpoint AS t
  USING (
    SELECT 'orders' AS source_table, {initial_watermark} AS last_event_id, current_timestamp() AS updated_at
  ) AS s
  ON t.source_table = s.source_table
  WHEN MATCHED THEN UPDATE SET last_event_id = s.last_event_id, updated_at = s.updated_at
  WHEN NOT MATCHED THEN INSERT (source_table, last_event_id, updated_at) VALUES (s.source_table, s.last_event_id, s.updated_at)
""")

print(f"Initial snapshot complete: {row_count} rows in Silver, watermark={initial_watermark}")

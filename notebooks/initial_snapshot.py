# Databricks notebook source
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
ep_data = w.api_client.do("GET", f"/api/2.0/postgres/{endpoint_name}")
pg_host = ep_data["status"]["hosts"]["host"]
cred_data = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_name})
pg_token = cred_data["token"]

# Check if Silver already has data — skip if so
silver_count = spark.sql(f"SELECT count(*) FROM {catalog}.{schema}.silver_orders").collect()[0][0]
if silver_count > 0:
    print(f"Silver already has {silver_count} rows — skipping initial snapshot.")
    dbutils.notebook.exit("skipped")

# 1. Record the initial event watermark
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

# 2. Snapshot app.orders into Silver
orders_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "app.orders")
    .option("user", lakebase_role)
    .option("password", pg_token)
    .option("sslmode", "require")
    .load()
)
row_count = orders_df.count()
print(f"Snapshotting {row_count} rows from app.orders into Silver...")

(
    orders_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.silver_orders")
)

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

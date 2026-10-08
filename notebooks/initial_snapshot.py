# Databricks notebook source
# ---- Initial snapshot: copy app.orders into Silver, record starting watermark ----
# See design doc: "Initial load and cutover" section.

import json
import urllib.request

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
lakebase_project = dbutils.widgets.get("lakebase_project")
lakebase_branch = dbutils.widgets.get("lakebase_branch")
lakebase_endpoint = dbutils.widgets.get("lakebase_endpoint")
lakebase_database = dbutils.widgets.get("lakebase_database")
lakebase_role = dbutils.widgets.get("lakebase_role")

# --- Get Lakebase connection via REST API (urllib) ---
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_url = ctx.apiUrl().get()
api_token = ctx.apiToken().get()

endpoint_name = f"projects/{lakebase_project}/branches/{lakebase_branch}/endpoints/{lakebase_endpoint}"

req1 = urllib.request.Request(
    f"{api_url}/api/2.0/postgres/{endpoint_name}",
    headers={"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"},
)
with urllib.request.urlopen(req1) as resp1:
    ep_data = json.loads(resp1.read())
pg_host = ep_data["status"]["hosts"]["host"]

req2 = urllib.request.Request(
    f"{api_url}/api/2.0/postgres/credentials",
    data=json.dumps({"endpoint": endpoint_name}).encode(),
    headers={"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"},
)
with urllib.request.urlopen(req2) as resp2:
    cred_data = json.loads(resp2.read())
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

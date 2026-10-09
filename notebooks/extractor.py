# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade
# COMMAND ----------
# MAGIC dbutils.library.restartPython()
# COMMAND ----------
# ---- Micro-batch extractor: reads cdc.change_log via JDBC, appends to Bronze Delta ----
# See design doc: "Databricks ingestion pattern" section.

from databricks.sdk import WorkspaceClient

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
lakebase_project = dbutils.widgets.get("lakebase_project")
lakebase_branch = dbutils.widgets.get("lakebase_branch")
lakebase_endpoint = dbutils.widgets.get("lakebase_endpoint")
lakebase_database = dbutils.widgets.get("lakebase_database")
lakebase_role = dbutils.widgets.get("lakebase_role")
batch_size = int(dbutils.widgets.get("batch_size"))

endpoint_name = f"projects/{lakebase_project}/branches/{lakebase_branch}/endpoints/{lakebase_endpoint}"

w = WorkspaceClient()
ep = w.postgres.get_endpoint(name=endpoint_name)
pg_host = ep.status.hosts.host
cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
pg_token = cred.token
print(f"Connected to Lakebase: {pg_host}")

# 1. Read the last watermark from the checkpoint table
watermark_row = spark.sql(f"""
  SELECT COALESCE(max(last_event_id), 0) AS last_event_id
  FROM {catalog}.{schema}.ingestion_checkpoint
  WHERE source_table = 'orders'
""").collect()[0]
last_event_id = watermark_row["last_event_id"]
print(f"Last watermark: {last_event_id}")

# 2. Query change_log via JDBC — bounded batch after the watermark
jdbc_url = f"jdbc:postgresql://{pg_host}:5432/{lakebase_database}"
query = (
    f"(SELECT event_id, source_schema, source_table, operation, "
    f"primary_key::text, old_row::text, new_row::text, changed_at, transaction_id "
    f"FROM cdc.change_log WHERE event_id > {last_event_id} "
    f"ORDER BY event_id LIMIT {batch_size}) AS t"
)

batch_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .option("user", lakebase_role)
    .option("password", pg_token)
    .option("sslmode", "require")
    .load()
).cache()

batch_count = batch_df.count()
print(f"Extracted {batch_count} events from change_log")

if batch_count == 0:
    print("No new events — nothing to do.")
    dbutils.notebook.exit("empty")

# Compute max event_id from the cached DataFrame (single JDBC query)
new_watermark = batch_df.agg({"event_id": "max"}).collect()[0][0]

# 3. Append to Bronze Delta table (at-least-once: duplicates are OK, dedup downstream)
from pyspark.sql.functions import current_timestamp

(
    batch_df
    .withColumn("ingested_at", current_timestamp())
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{catalog}.{schema}.bronze_change_log")
)
print(f"Appended {batch_count} events to Bronze")

batch_df.unpersist()

spark.sql(f"""
  MERGE INTO {catalog}.{schema}.ingestion_checkpoint AS t
  USING (
    SELECT 'orders' AS source_table, {new_watermark} AS last_event_id, current_timestamp() AS updated_at
  ) AS s
  ON t.source_table = s.source_table
  WHEN MATCHED THEN UPDATE SET last_event_id = s.last_event_id, updated_at = s.updated_at
  WHEN NOT MATCHED THEN INSERT (source_table, last_event_id, updated_at) VALUES (s.source_table, s.last_event_id, s.updated_at)
""")

print(f"Watermark advanced to {new_watermark}")
print(f"Extraction complete: {batch_count} events ingested to Bronze.")

# Databricks notebook source
# ---- Reconciliation: compare Lakebase source table with Databricks Silver table ----
# See design doc: "Initial load and cutover" step 5 and "Operational controls".

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
jdbc_url = f"jdbc:postgresql://{pg_host}:5432/{lakebase_database}"

# --- 1. Read source table from Lakebase ---
source_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "app.orders")
    .option("user", lakebase_role)
    .option("password", pg_token)
    .option("sslmode", "require")
    .load()
)

source_count = source_df.count()
silver_count = spark.sql(f"SELECT count(*) FROM {catalog}.{schema}.silver_orders").collect()[0][0]

print(f"=== Row Count Reconciliation ===")
print(f"Source (Lakebase app.orders): {source_count}")
print(f"Silver (Databricks):          {silver_count}")
print(f"Match: {source_count == silver_count}")

if source_count != silver_count:
    print("MISMATCH: Row counts differ!")

# --- 2. Primary key set comparison ---
source_keys = source_df.select("order_id").distinct()
silver_keys = spark.sql(f"SELECT order_id FROM {catalog}.{schema}.silver_orders").distinct()

source_key_set = set(row.order_id for row in source_keys.collect())
silver_key_set = set(row.order_id for row in silver_keys.collect())

missing_in_silver = source_key_set - silver_key_set
extra_in_silver = silver_key_set - source_key_set

print(f"\n=== Primary Key Set Reconciliation ===")
print(f"Source keys: {len(source_key_set)}")
print(f"Silver keys: {len(silver_key_set)}")
print(f"Missing in Silver: {sorted(missing_in_silver) if missing_in_silver else 'none'}")
print(f"Extra in Silver: {sorted(extra_in_silver) if extra_in_silver else 'none'}")

if missing_in_silver or extra_in_silver:
    print("MISMATCH: Key sets differ!")

# --- 3. Row hash comparison ---
from pyspark.sql.functions import sha2, concat_ws, col

source_hashed = (
    source_df
    .withColumn("row_hash", sha2(concat_ws("|",
        col("order_id"), col("customer_id"), col("status"),
        col("total"), col("created_at"), col("updated_at")), 256))
    .select("order_id", "row_hash")
)

silver_hashed = (
    spark.sql(f"SELECT * FROM {catalog}.{schema}.silver_orders")
    .withColumn("row_hash", sha2(concat_ws("|",
        col("order_id"), col("customer_id"), col("status"),
        col("total"), col("created_at"), col("updated_at")), 256))
    .select("order_id", "row_hash")
)

mismatches = source_hashed.alias("s").join(
    silver_hashed.alias("t"),
    col("s.order_id") == col("t.order_id"),
    "fullOuter"
).where(
    (col("s.row_hash").isNull()) | (col("t.row_hash").isNull()) | (col("s.row_hash") != col("t.row_hash"))
).select(
    col("s.order_id").alias("source_order_id"),
    col("t.order_id").alias("silver_order_id"),
    col("s.row_hash").alias("source_hash"),
    col("t.row_hash").alias("silver_hash"),
)

mismatch_count = mismatches.count()

print(f"\n=== Row Hash Reconciliation ===")
print(f"Mismatched rows: {mismatch_count}")

if mismatch_count > 0:
    print("MISMATCH: Row hashes differ for the following rows:")
    mismatches.show(truncate=False)
else:
    print("All shared rows have matching hashes.")

# --- Summary ---
all_match = (source_count == silver_count) and not missing_in_silver and not extra_in_silver and (mismatch_count == 0)

print(f"\n=== Reconciliation Summary ===")
if all_match:
    print("PASS: Source and Silver tables are identical.")
else:
    print("FAIL: Source and Silver tables differ. See details above.")

dbutils.notebook.exit("pass" if all_match else "fail")

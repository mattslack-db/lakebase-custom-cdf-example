# Databricks notebook source
# ---- Create Bronze, Silver, and checkpoint tables if they don't exist ----
# This notebook is idempotent — safe to run multiple times.

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# Bronze: raw event envelope from cdc.change_log
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_change_log (
  event_id       BIGINT,
  source_schema  STRING,
  source_table   STRING,
  operation      STRING,
  primary_key    STRING,
  old_row        STRING,
  new_row        STRING,
  changed_at     TIMESTAMP,
  transaction_id STRING,
  ingested_at    TIMESTAMP
) USING DELTA
""")

# Silver: current-state table for app.orders
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_orders (
  order_id    BIGINT,
  customer_id BIGINT,
  status      STRING,
  total       DECIMAL(12, 2),
  created_at  TIMESTAMP,
  updated_at  TIMESTAMP
) USING DELTA
""")

# Checkpoint: tracks the last extracted event_id per source table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ingestion_checkpoint (
  source_table   STRING,
  last_event_id  BIGINT,
  updated_at     TIMESTAMP
) USING DELTA
""")

# Checkpoint for silver merge: tracks the last Bronze event_id applied to Silver
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_checkpoint (
  source_table   STRING,
  last_event_id  BIGINT,
  updated_at     TIMESTAMP
) USING DELTA
""")

print(f"Tables ready in {catalog}.{schema}")

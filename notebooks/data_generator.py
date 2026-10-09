# Databricks notebook source
# ---- Data generator: continuously makes pseudo-random INSERT/UPDATE/DELETE on app.orders ----
# The trigger on app.orders captures every change into cdc.change_log.

import random
import time
import psycopg2
from datetime import datetime
from databricks.sdk import WorkspaceClient

lakebase_project = dbutils.widgets.get("lakebase_project")
lakebase_branch = dbutils.widgets.get("lakebase_branch")
lakebase_endpoint = dbutils.widgets.get("lakebase_endpoint")
lakebase_database = dbutils.widgets.get("lakebase_database")
lakebase_role = dbutils.widgets.get("lakebase_role")

run_minutes = int(dbutils.widgets.get("run_minutes"))
interval_seconds = int(dbutils.widgets.get("interval_seconds"))

endpoint_name = f"projects/{lakebase_project}/branches/{lakebase_branch}/endpoints/{lakebase_endpoint}"

_w = None
_pg_host = None
_pg_token = None
_token_acquired_at = 0.0
TOKEN_REFRESH_SECONDS = 3000

def _get_lakebase_connection():
    global _w
    if _w is None:
        _w = WorkspaceClient()
    ep_data = _w.api_client.do("GET", f"/api/2.0/postgres/{endpoint_name}")
    host = ep_data["status"]["hosts"]["host"]
    cred_data = _w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_name})
    token = cred_data["token"]
    return host, token

def get_pg_connection():
    global _pg_host, _pg_token, _token_acquired_at
    now = time.time()
    if _pg_host is None or now - _token_acquired_at > TOKEN_REFRESH_SECONDS:
        _pg_host, _pg_token = _get_lakebase_connection()
        _token_acquired_at = now
    return psycopg2.connect(
        host=_pg_host,
        user=lakebase_role,
        password=_pg_token,
        dbname=lakebase_database,
        sslmode="require",
    )

def random_insert(conn):
    customer_id = random.randint(1000, 9999)
    status = random.choice(["pending", "shipped", "delivered", "cancelled"])
    total = round(random.uniform(10.0, 500.0), 2)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO app.orders (customer_id, status, total) VALUES (%s, %s, %s)",
            (customer_id, status, total),
        )
    conn.commit()
    print(f"[{datetime.now()}] INSERT customer_id={customer_id} status={status} total={total}")

def random_update(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT order_id FROM app.orders ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if row is None:
            return
        order_id = row[0]
        new_status = random.choice(["pending", "shipped", "delivered", "cancelled"])
        new_total = round(random.uniform(10.0, 500.0), 2)
        cur.execute(
            "UPDATE app.orders SET status = %s, total = %s, updated_at = now() WHERE order_id = %s",
            (new_status, new_total, order_id),
        )
    conn.commit()
    print(f"[{datetime.now()}] UPDATE order_id={order_id} status={new_status} total={new_total}")

def random_delete(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT order_id FROM app.orders ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if row is None:
            return
        order_id = row[0]
        cur.execute("DELETE FROM app.orders WHERE order_id = %s", (order_id,))
    conn.commit()
    print(f"[{datetime.now()}] DELETE order_id={order_id}")

end_time = time.time() + run_minutes * 60
ops = [random_insert, random_update, random_delete]

conn = None
while time.time() < end_time:
    try:
        if conn is None or conn.closed:
            conn = get_pg_connection()
        op = random.choices(ops, weights=[40, 40, 20], k=1)[0]
        op(conn)
    except Exception as e:
        print(f"[{datetime.now()}] ERROR: {e}")
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            conn = None
    time.sleep(interval_seconds)

if conn is not None:
    try:
        conn.close()
    except Exception:
        pass

print(f"[{datetime.now()}] Data generator finished after {run_minutes} minutes.")

# Databricks notebook source
# ---- Data generator: continuously makes pseudo-random INSERT/UPDATE/DELETE on app.orders ----
# The trigger on app.orders captures every change into cdc.change_log.

import random
import time
import json
import urllib.request
import psycopg2
from datetime import datetime

lakebase_project = dbutils.widgets.get("lakebase_project")
lakebase_branch = dbutils.widgets.get("lakebase_branch")
lakebase_endpoint = dbutils.widgets.get("lakebase_endpoint")
lakebase_database = dbutils.widgets.get("lakebase_database")
lakebase_role = dbutils.widgets.get("lakebase_role")

run_minutes = int(dbutils.widgets.get("run_minutes"))
interval_seconds = int(dbutils.widgets.get("interval_seconds"))

def get_lakebase_connection():
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
    host = ep_data["status"]["hosts"]["host"]

    req2 = urllib.request.Request(
        f"{api_url}/api/2.0/postgres/credentials",
        data=json.dumps({"endpoint": endpoint_name}).encode(),
        headers={"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req2) as resp2:
        cred_data = json.loads(resp2.read())
    token = cred_data["token"]

    return host, token

def get_pg_connection():
    host, token = get_lakebase_connection()
    return psycopg2.connect(
        host=host,
        user=lakebase_role,
        password=token,
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

while time.time() < end_time:
    try:
        conn = get_pg_connection()
        try:
            op = random.choices(ops, weights=[40, 40, 20], k=1)[0]
            op(conn)
        except Exception as e:
            print(f"[{datetime.now()}] ERROR: {e}")
            conn.rollback()
        finally:
            conn.close()
    except Exception as e:
        print(f"[{datetime.now()}] CONNECTION ERROR: {e}")
    time.sleep(interval_seconds)

print(f"[{datetime.now()}] Data generator finished after {run_minutes} minutes.")

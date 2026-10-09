# Databricks notebook source
# ---- Shared Lakebase connection helper using Databricks SDK ----
# Inlined in each notebook because DAB notebook path resolution makes
# %run and sys.path imports unreliable across deployed paths.
# This file is kept for reference/local testing only.

from databricks.sdk import WorkspaceClient

def get_lakebase_connection(endpoint_name):
    """Return (host, token) for a Lakebase Postgres endpoint."""
    w = WorkspaceClient()
    ep_data = w.api_client.do("GET", f"/api/2.0/postgres/{endpoint_name}")
    host = ep_data["status"]["hosts"]["host"]
    cred_data = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_name})
    token = cred_data["token"]
    return host, token

# Lakebase Custom CDF Example

Trigger-based change data capture from [Lakebase Postgres](https://docs.databricks.com/aws/en/oltp/) into Delta tables in Unity Catalog, deployed as a Databricks Asset Bundle.

A Postgres trigger records every `INSERT`, `UPDATE` and `DELETE` on a source table into an append-only `cdc.change_log` table. A scheduled job pulls new events over JDBC into a Bronze Delta table and applies them to a Silver current-state table. Silver is built two ways so they can be compared:

- **Notebook MERGE** (`silver_orders`) — explicit dedup, ordering and `MERGE` logic.
- **Auto CDC pipeline** (`silver_orders_auto_cdc`) — a Lakeflow Declarative Pipeline using `create_auto_cdc_flow` (SCD Type 1).

Both are reconciled against the source table on every run.

```
Lakebase Postgres                          Databricks (Unity Catalog)
┌─────────────┐  trigger  ┌────────────────┐   JDBC   ┌───────────────────┐   ┌──────────────────────────┐
│ app.orders  │ ────────▶ │ cdc.change_log │ ───────▶ │ bronze_change_log │ ─▶│ silver_orders (MERGE)    │
└─────────────┘           └────────────────┘ extractor└───────────────────┘   │ silver_orders_auto_cdc   │
                                                                              └──────────────────────────┘
```

## Repository layout

| Path | Purpose |
|------|---------|
| `sql/postgres/` | Lakebase DDL: `cdc.change_log`, the demo `app.orders` table, and the capture trigger |
| `notebooks/setup_tables.py` | Creates the Bronze, Silver and checkpoint Delta tables (idempotent) |
| `notebooks/initial_snapshot.py` | One-time copy of `app.orders` into Silver and records the starting watermark |
| `notebooks/extractor.py` | Reads change-log events after the last watermark and appends them to Bronze |
| `notebooks/silver_merge.py` | Applies Bronze events to `silver_orders` with `MERGE` |
| `src/pipelines/silver_auto_cdc/` | Auto CDC pipeline that builds `silver_orders_auto_cdc` |
| `notebooks/reconcile.py` | Compares a Silver table with the Lakebase source |
| `notebooks/data_generator.py` | Makes random changes to `app.orders` to drive the demo |
| `resources/` | Bundle job and pipeline definitions |
| `scripts/deploy.sh` | Validates and deploys the bundle using values from `.env` |

## Prerequisites

- A Databricks workspace with Unity Catalog and Lakebase enabled
- A Lakebase project (defaults: project `custom-cdf-demo`, branch `production`, endpoint `primary`, database `databricks_postgres`)
- [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/) with a configured profile
- `psql` or another Postgres client to run the setup SQL

## Setup

1. **Create the Lakebase objects.** Connect to your Lakebase database and run the SQL files in order:

   ```bash
   psql "<lakebase connection string>" -f sql/postgres/01_cdc_schema.sql
   psql "<lakebase connection string>" -f sql/postgres/02_demo_orders.sql
   psql "<lakebase connection string>" -f sql/postgres/03_trigger.sql
   ```

2. **Configure your environment.**

   ```bash
   cp .env.template .env
   ```

   Edit `.env`:

   | Key | Description |
   |-----|-------------|
   | `databricks_profile` | CLI profile from `~/.databrickscfg` |
   | `catalog` | Unity Catalog catalog for the Bronze/Silver tables |
   | `lakebase_role` | Postgres role used for JDBC (your user email or `admin`) |

   Every key except `databricks_profile` is passed to the bundle as `--var key=value`, so any variable in `databricks.yml` can be overridden here.

3. **Deploy.**

   ```bash
   ./scripts/deploy.sh            # target dev, profile from .env
   ./scripts/deploy.sh dev myprof # explicit target and profile
   ```

## Running

```bash
databricks bundle run data_generator -t dev --profile <profile>      # generate source changes
databricks bundle run ingestion_pipeline -t dev --profile <profile>  # extract, merge, reconcile
```

The ingestion job is scheduled every minute but deployed **paused**. Unpause it in the Jobs UI to run continuously.

## How it works

- **Ordering and replay.** `event_id` is an identity column, so the extractor reads a bounded batch after a stored watermark. Bronze writes are at-least-once; the MERGE notebook dedupes by `event_id` and keeps the latest event per key, and Auto CDC sequences by `event_id` so replays resolve identically.
- **Cutover.** The initial snapshot reads the change-log watermark *before* copying `app.orders`, so every event at or below that watermark is already reflected in Silver.
- **Authentication.** Notebooks get the Lakebase host and a short-lived OAuth token from the Databricks SDK (`w.postgres`). No passwords are stored.

## License

[Apache License 2.0](LICENSE)

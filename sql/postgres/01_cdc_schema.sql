-- CDC change_log table — shared append-only envelope for all source tables.
-- See design doc: "Change-log data model" section.

CREATE SCHEMA IF NOT EXISTS cdc;

CREATE TABLE IF NOT EXISTS cdc.change_log (
  event_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source_schema  TEXT NOT NULL,
  source_table   TEXT NOT NULL,
  operation      CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D')),
  primary_key    JSONB NOT NULL,
  old_row        JSONB,
  new_row        JSONB,
  changed_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  transaction_id TEXT,
  processed_at   TIMESTAMPTZ,
  CONSTRAINT change_log_payload_ck CHECK (
    (operation = 'I' AND new_row IS NOT NULL AND old_row IS NULL) OR
    (operation = 'U' AND new_row IS NOT NULL) OR
    (operation = 'D' AND old_row IS NOT NULL AND new_row IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS change_log_extract_idx
  ON cdc.change_log (event_id);

CREATE INDEX IF NOT EXISTS change_log_table_event_idx
  ON cdc.change_log (source_schema, source_table, event_id);

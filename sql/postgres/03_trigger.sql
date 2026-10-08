-- Trigger function: captures INSERT/UPDATE/DELETE on app.orders into cdc.change_log.
-- The trigger runs AFTER ROW in the same transaction as the source mutation.
-- See design doc: "Trigger implementation example" section.

CREATE OR REPLACE FUNCTION cdc.capture_orders_change()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, cdc, app
AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO cdc.change_log (
      source_schema, source_table, operation, primary_key,
      old_row, new_row, transaction_id
    )
    VALUES (
      TG_TABLE_SCHEMA,
      TG_TABLE_NAME,
      'I',
      jsonb_build_object('order_id', NEW.order_id),
      NULL,
      to_jsonb(NEW),
      txid_current()::text
    );
    RETURN NEW;

  ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO cdc.change_log (
      source_schema, source_table, operation, primary_key,
      old_row, new_row, transaction_id
    )
    VALUES (
      TG_TABLE_SCHEMA,
      TG_TABLE_NAME,
      'U',
      jsonb_build_object('order_id', NEW.order_id),
      to_jsonb(OLD),
      to_jsonb(NEW),
      txid_current()::text
    );
    RETURN NEW;

  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO cdc.change_log (
      source_schema, source_table, operation, primary_key,
      old_row, new_row, transaction_id
    )
    VALUES (
      TG_TABLE_SCHEMA,
      TG_TABLE_NAME,
      'D',
      jsonb_build_object('order_id', OLD.order_id),
      to_jsonb(OLD),
      NULL,
      txid_current()::text
    );
    RETURN OLD;
  END IF;
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS orders_capture_changes ON app.orders;

CREATE TRIGGER orders_capture_changes
AFTER INSERT OR UPDATE OR DELETE ON app.orders
FOR EACH ROW
EXECUTE FUNCTION cdc.capture_orders_change();

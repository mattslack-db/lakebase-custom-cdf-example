-- Demo source table: app.orders
-- This is a representative business table to demonstrate trigger-based change capture.

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.orders (
  order_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'pending',
  total       NUMERIC(12, 2) NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Sample data
INSERT INTO app.orders (customer_id, status, total)
VALUES
  (1001, 'pending', 49.99),
  (1002, 'shipped', 125.00),
  (1003, 'pending', 78.50),
  (1004, 'cancelled', 0.00),
  (1005, 'shipped', 299.97)
ON CONFLICT DO NOTHING;

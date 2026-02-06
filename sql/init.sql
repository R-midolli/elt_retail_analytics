CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.staging_sales;

CREATE TABLE raw.staging_sales (
  invoice_no          TEXT,
  stock_code          TEXT,
  product_description TEXT,
  quantity            INTEGER,
  unit_price          NUMERIC(12,4),
  invoice_date        TIMESTAMP,
  customer_id         BIGINT,
  customer_name       TEXT,
  customer_city       TEXT,
  country_original    TEXT,
  country_fr          TEXT,
  is_cancellation     BOOLEAN,
  is_return           BOOLEAN
);

COPY raw.staging_sales (
  invoice_no, stock_code, product_description,
  quantity, unit_price, invoice_date,
  customer_id, customer_name, customer_city,
  country_original, country_fr,
  is_cancellation, is_return
)
FROM '/tmp/sales_data.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');


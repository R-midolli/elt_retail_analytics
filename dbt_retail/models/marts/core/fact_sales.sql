{{ config(materialized='table', schema='marts') }}

select
    invoice_no,
    invoice_day as invoice_day,
    invoice_ts,
    customer_id,
    stock_code,
    country,
    quantity,
    unit_price,
    line_amount
from {{ ref('stg_sales') }}
where invoice_no is not null
  and stock_code is not null

{{ config(materialized='table', schema='marts') }}

with base as (
    select
        invoice_no,
        cast(invoice_ts as date) as sales_date,
        customer_id,
        stock_code,
        country,
        quantity,
        unit_price,
        line_amount,
        (left(invoice_no, 1) = 'C' or quantity < 0) as is_cancelled
    from {{ ref('stg_sales') }}
    where invoice_no is not null
      and stock_code is not null
      and customer_id is not null
      and invoice_ts is not null
)

select *
from base

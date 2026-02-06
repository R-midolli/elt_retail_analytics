{{ config(materialized='view', schema='analytics_marts', tags=['core']) }}

select
    invoice_no,
    stock_code,
    customer_id,
    invoice_date,
    invoice_date::date as sales_date,
    quantity,
    unit_price,
    line_amount
from {{ ref('fact_sales') }}

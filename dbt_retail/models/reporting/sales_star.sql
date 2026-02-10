{{ config(materialized='view', schema='analytics_reporting') }}

select
    fs.invoice_no,
    fs.sales_date,
    fs.customer_id,
    fs.stock_code,
    fs.quantity,
    fs.unit_price,
    fs.line_amount,

    p.product_description,
    i.country,

    d.date_day,
    d.year,
    d.month,
    d.year_month,
    d.iso_week,
    d.dow
from {{ ref('fact_sales_star') }} fs
left join {{ ref('dim_products') }} p
    on fs.stock_code = p.stock_code
left join {{ ref('dim_invoice') }} i
    on fs.invoice_no = i.invoice_no
left join {{ ref('dim_date') }} d
    on fs.sales_date = d.date_day

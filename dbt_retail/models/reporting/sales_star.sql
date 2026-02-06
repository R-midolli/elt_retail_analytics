{{ config(materialized='view', schema='analytics_reporting', tags=['reporting']) }}

select
    fs.invoice_no,
    fs.invoice_date,
    fs.sales_date,
    fs.customer_id,
    c.customer_name,
    c.customer_city,
    c.country_fr,
    fs.stock_code,
    p.product_description,
    fs.quantity,
    fs.unit_price,
    fs.line_amount,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week_start_date
from {{ ref('fact_sales_star') }} fs
left join {{ ref('dim_customers') }} c
  on fs.customer_id = c.customer_id
left join {{ ref('dim_products') }} p
  on fs.stock_code = p.stock_code
left join {{ ref('dim_date') }} d
  on fs.sales_date = d.date_day

{{ config(materialized='table', schema='reporting') }}

with base as (
    select
        fs.invoice_no,
        fs.stock_code,
        fs.sales_date,
        fs.quantity,
        fs.line_amount,
        case
            when fs.quantity < 0 or fs.invoice_no like 'C%' then true
            else false
        end as is_cancellation,
        p.product_description
    from {{ ref('fact_sales_star') }} fs
    left join {{ ref('dim_products') }} p
      on fs.stock_code = p.stock_code
),

filtered as (
    select *
    from base
    where is_cancellation = false
      and quantity > 0
),

agg as (
    select
        stock_code,
        max(product_description)   as product_description,
        count(distinct invoice_no) as orders,
        sum(quantity)              as units,
        sum(line_amount)           as revenue
    from filtered
    group by 1
)

select
    stock_code,
    product_description,
    orders,
    units,
    revenue,
    case when orders = 0 then null else revenue / orders end as aov
from agg

{{ config(materialized='table', schema='reporting') }}

with base as (
    select
        invoice_no,
        sales_date,
        quantity,
        line_amount,
        case
            when quantity < 0 or invoice_no like 'C%' then true
            else false
        end as is_cancellation
    from {{ ref('fact_sales_star') }}
),

filtered as (
    select *
    from base
    where is_cancellation = false
      and quantity > 0
),

agg as (
    select
        sales_date,
        count(distinct invoice_no) as orders,
        sum(quantity)              as units,
        sum(line_amount)           as revenue
    from filtered
    group by 1
)

select
    sales_date,
    orders,
    units,
    revenue,
    case when orders = 0 then null else revenue / orders end as aov
from agg
order by sales_date

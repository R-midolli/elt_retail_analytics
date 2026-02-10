{{ config(materialized='table', schema='reporting') }}

with base as (
    select
        invoice_no,
        customer_id,
        sales_date,
        quantity,
        line_amount,
        case
            when quantity < 0 or invoice_no like 'C%' then true
            else false
        end as is_cancellation
    from {{ ref('fact_sales_star') }}
    where customer_id is not null
),

filtered as (
    select *
    from base
    where is_cancellation = false
      and quantity > 0
),

agg as (
    select
        customer_id,
        min(sales_date)            as first_purchase_date,
        max(sales_date)            as last_purchase_date,
        count(distinct invoice_no) as orders,
        sum(quantity)              as units,
        sum(line_amount)           as revenue
    from filtered
    group by 1
)

select
    customer_id,
    first_purchase_date,
    last_purchase_date,
    orders,
    units,
    revenue,
    case when orders = 0 then null else revenue / orders end as aov
from agg

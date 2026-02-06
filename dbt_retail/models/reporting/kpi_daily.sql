{{ config(materialized='table', schema='reporting', tags=['reporting']) }}

with base as (
    select
        date_trunc('day', invoice_date)::date as sales_date,
        invoice_no,
        customer_id,
        stock_code,
        quantity,
        line_amount,
        is_cancellation,
        is_return
    from {{ ref('stg_sales') }}
)

select
    sales_date,

    count(*) as line_count,
    count(distinct invoice_no) as invoice_count,
    count(distinct customer_id) filter (where customer_id is not null) as customer_count,

    sum(quantity) filter (where not is_cancellation and not is_return) as units_sold,
    sum(line_amount) filter (where not is_cancellation and not is_return) as revenue_sales,

    sum(abs(line_amount)) filter (where is_return) as returns_value,
    sum(abs(line_amount)) filter (where is_cancellation) as cancellations_value,

    sum(line_amount) as net_amount_raw

from base
group by sales_date
order by sales_date

{{ config(materialized='table', schema='reporting', tags=['reporting']) }}

with base as (
    select
        stock_code,
        product_description,
        invoice_no,
        quantity,
        line_amount,
        is_cancellation,
        is_return
    from {{ ref('stg_sales') }}
    where stock_code is not null
)

select
    stock_code,
    max(product_description) as product_description,

    count(*) as line_count,
    count(distinct invoice_no) as invoice_count,

    sum(quantity) filter (where not is_cancellation and not is_return) as units_sold,
    sum(line_amount) filter (where not is_cancellation and not is_return) as revenue_sales,

    count(*) filter (where is_return) as return_lines,
    count(*) filter (where is_cancellation) as cancellation_lines,

    sum(abs(line_amount)) filter (where is_return) as returns_value,
    sum(abs(line_amount)) filter (where is_cancellation) as cancellations_value,

    case
        when count(*) = 0 then 0
        else (count(*) filter (where is_return))::numeric / count(*)::numeric
    end as return_rate_lines

from base
group by stock_code
order by revenue_sales desc nulls last

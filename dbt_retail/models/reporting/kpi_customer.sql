{{ config(materialized='table', schema='reporting', tags=['reporting']) }}

with base as (
    select
        customer_id,
        customer_name,
        customer_city,
        country_fr,
        invoice_no,
        invoice_date,
        quantity,
        line_amount,
        is_cancellation,
        is_return
    from {{ ref('stg_sales') }}
    where customer_id is not null
)

select
    customer_id,
    max(customer_name) as customer_name,
    max(customer_city) as customer_city,
    max(country_fr) as country_fr,

    count(distinct invoice_no) filter (where not is_cancellation and not is_return) as invoice_count,
    sum(line_amount) filter (where not is_cancellation and not is_return) as revenue_sales,

    min(invoice_date) filter (where not is_cancellation and not is_return) as first_purchase_ts,
    max(invoice_date) filter (where not is_cancellation and not is_return) as last_purchase_ts,

    case
        when count(distinct invoice_no) filter (where not is_cancellation and not is_return) = 0 then null
        else
            (sum(line_amount) filter (where not is_cancellation and not is_return))::numeric
            / (count(distinct invoice_no) filter (where not is_cancellation and not is_return))::numeric
    end as avg_basket_value

from base
group by customer_id
order by revenue_sales desc nulls last

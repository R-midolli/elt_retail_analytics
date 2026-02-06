{{ config(materialized='table', schema='analytics_marts', tags=['core']) }}

with base as (
    select
        invoice_no,
        invoice_date::date as invoice_day,

        -- KPI invoice-level
        max(customer_id) as customer_id,
        max(country_fr) as country_fr,

        -- flags (si au moins une ligne est cancellation/return)
        bool_or(coalesce(is_cancellation, false)) as has_cancellation,
        bool_or(coalesce(is_return, false))       as has_return,

        -- agrégats invoice
        count(*)                                   as lines_count,
        sum(quantity)                              as items_count,
        sum(line_amount)::numeric(12,4)            as invoice_amount
    from {{ ref('stg_sales') }}
    where invoice_no is not null
    group by 1,2
)

select
    invoice_no,
    invoice_day,
    customer_id,
    country_fr,
    has_cancellation,
    has_return,
    lines_count,
    items_count,
    invoice_amount
from base

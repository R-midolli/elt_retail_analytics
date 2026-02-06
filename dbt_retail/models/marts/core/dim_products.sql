{{ config(materialized='table', schema='analytics_marts', tags=['core']) }}

with base as (
    select
        upper(trim(stock_code))                       as stock_code,
        nullif(trim(product_description), '')         as product_description,
        invoice_date
    from {{ ref('stg_sales') }}
    where stock_code is not null
),

ranked as (
    select
        stock_code,
        product_description,
        row_number() over (
            partition by stock_code
            order by
                (product_description is not null) desc,
                invoice_date desc nulls last,
                length(product_description) desc nulls last,
                product_description desc nulls last
        ) as rn
    from base
)

select
    stock_code,
    coalesce(product_description, 'Unknown product') as product_description
from ranked
where rn = 1

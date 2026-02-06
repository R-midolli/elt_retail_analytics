{{ config(materialized='view', schema='analytics_staging', tags=['staging']) }}

with src as (
    select *
    from {{ source('raw', 'staging_sales') }}
),

clean as (
    select
        nullif(trim(invoice_no::text), '')                      as invoice_no,

        -- ✅ NORMALISATION CLÉ : UPPER + TRIM
        nullif(upper(trim(stock_code::text)), '')              as stock_code,

        nullif(trim(product_description::text), '')            as product_description,

        cast(quantity as integer)                              as quantity,
        cast(unit_price as numeric(12,4))                      as unit_price,
        cast(invoice_date as timestamp)                        as invoice_date,

        case
            when nullif(trim(customer_id::text), '') ~ '^\d+$'
                then nullif(trim(customer_id::text), '')::bigint
            else null
        end                                                    as customer_id,

        nullif(trim(customer_name::text), '')                  as customer_name,
        nullif(trim(customer_city::text), '')                  as customer_city,
        nullif(trim(country_original::text), '')               as country_original,
        nullif(trim(country_fr::text), '')                     as country_fr,

        coalesce(is_cancellation, false)                       as is_cancellation,
        coalesce(is_return, false)                             as is_return
    from src
)

select
    invoice_no,
    stock_code,
    product_description,
    quantity,
    unit_price,
    invoice_date,
    customer_id,
    customer_name,
    customer_city,
    country_original,
    country_fr,
    is_cancellation,
    is_return,
    (quantity::numeric * unit_price)::numeric(12,4)            as line_amount
from clean

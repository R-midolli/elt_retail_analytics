{{ config(materialized='view', schema='staging') }}

with source as (
    select *
    from {{ source('raw_retail', 'sales') }}
),

typed as (
    select
        trim(cast(invoice_no as text))                    as invoice_no,
        upper(trim(cast(stock_code as text)))             as stock_code,
        cast(customer_id as bigint)                       as customer_id,

        nullif(trim(product_description), '')             as product_description,
        nullif(trim(country), '')                         as country,

        cast(quantity as int)                             as quantity,
        cast(unit_price as numeric(18,4))                 as unit_price,

        cast(invoice_date as timestamp)                   as invoice_ts,
        cast(invoice_date as date)                        as invoice_day
    from source
)

select
    invoice_no,
    stock_code,
    customer_id,
    product_description,
    country,
    quantity,
    unit_price,
    invoice_ts,
    invoice_day,
    (cast(quantity as numeric(18,4)) * unit_price)        as line_amount
from typed
where invoice_no is not null
  and stock_code is not null

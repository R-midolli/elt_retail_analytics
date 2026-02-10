{{ config(materialized='table', schema='marts') }}

with base as (
    select
        nullif(btrim(stock_code), '') as stock_code,
        nullif(btrim(product_description), '') as product_description_raw,
        invoice_ts
    from {{ ref('stg_sales') }}
    where stock_code is not null
),

ranked as (
    select
        stock_code,
        product_description_raw,
        row_number() over (
            partition by stock_code
            order by (product_description_raw is null) asc, invoice_ts desc nulls last
        ) as rn
    from base
),

dedup as (
    select
        stock_code,
        product_description_raw,

        -- normalisation pour détecter ????missing / ponctuation / etc.
        nullif(
          btrim(
            regexp_replace(
              lower(coalesce(product_description_raw,'')),
              '[^a-z0-9]+',
              ' ',
              'g'
            )
          ),
          ''
        ) as desc_norm
    from ranked
    where rn = 1
),

final as (
    select
        stock_code,

        case
            when desc_norm is null then 'Unknown'
            when desc_norm in ('manual','check') then 'Non-product'
            when desc_norm ~ '^\?+$' then 'Non-product'
            when desc_norm like '%missing%' then 'Non-product'
            when desc_norm like '%damag%' then 'Non-product'
            when desc_norm like '%damage%' then 'Non-product'
            else product_description_raw
        end as product_description,

        case
            when desc_norm is null then false
            when desc_norm in ('manual','check') then false
            when desc_norm ~ '^\?+$' then false
            when desc_norm like '%missing%' then false
            when desc_norm like '%damag%' then false
            when desc_norm like '%damage%' then false
            else true
        end as is_product
    from dedup
)

select * from final

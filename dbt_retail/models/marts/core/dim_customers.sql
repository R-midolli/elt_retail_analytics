{{ config(materialized='table', schema='marts') }}

with base as (
    select
        customer_id,
        max(country) as country
    from {{ ref('stg_sales') }}
    where customer_id is not null
    group by 1
)
select * from base

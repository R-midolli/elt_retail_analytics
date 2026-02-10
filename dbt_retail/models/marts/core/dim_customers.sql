with base as (
    select
        customer_id,
        max(country) as country
    from staging.stg_sales
    where customer_id is not null
    group by 1
)
select * from base

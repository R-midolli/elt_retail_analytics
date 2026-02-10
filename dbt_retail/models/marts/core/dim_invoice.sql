with base as (
    select
        invoice_no,
        min(invoice_ts)  as invoice_ts,
        min(invoice_day) as invoice_day,
        max(country)     as country,
        (invoice_no like 'C%') as is_cancelled
    from staging.stg_sales
    where invoice_no is not null
    group by 1
)
select * from base

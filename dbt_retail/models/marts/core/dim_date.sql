{{ config(materialized='table', schema='analytics_marts', tags=['core']) }}

with bounds as (
    select
        min(invoice_date)::date as min_date,
        max(invoice_date)::date as max_date
    from {{ ref('fact_sales') }}
),
dates as (
    select generate_series(min_date, max_date, interval '1 day')::date as date_day
    from bounds
)

select
    date_day,
    extract(isoyear from date_day)::int      as year,
    extract(quarter from date_day)::int     as quarter,
    extract(month from date_day)::int       as month,
    trim(to_char(date_day, 'Month'))        as month_name,
    extract(day from date_day)::int         as day_of_month,
    extract(isodow from date_day)::int      as iso_day_of_week,
    trim(to_char(date_day, 'Day'))          as day_name,
    (extract(isodow from date_day) in (6,7)) as is_weekend,
    date_trunc('week', date_day)::date      as week_start_date,
    date_trunc('month', date_day)::date     as month_start_date,
    date_trunc('quarter', date_day)::date   as quarter_start_date,
    date_trunc('year', date_day)::date      as year_start_date
from dates

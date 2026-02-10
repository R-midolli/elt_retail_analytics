{{ config(materialized='table', schema='marts') }}

with bounds as (
    select
        min(sales_date)::date as min_date,
        max(sales_date)::date as max_date
    from {{ ref('fact_sales_star') }}
    where sales_date is not null
),
spine as (
    select generate_series(min_date, max_date, interval '1 day')::date as date_day
    from bounds
)

select
    date_day,
    extract(year from date_day)::int as year,
    extract(month from date_day)::int as month,
    to_char(date_day, 'YYYY-MM') as year_month,
    extract(week from date_day)::int as iso_week,
    extract(isodow from date_day)::int as dow
from spine
order by date_day

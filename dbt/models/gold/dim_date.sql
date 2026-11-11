{{ config(location='s3://sme-lake/gold/dim_date.parquet') }}

-- Grain: one row per calendar day, from the first to the last date used by the facts.
-- The date itself is the key: facts store plain dates (issue_date, due_date...), easy to read and to join.
-- generate_series and strftime are DuckDB functions; on another engine, the dbt_utils.date_spine
-- macro would build the same calendar.

with bounds as (

    select min(day) as first_day, max(day) as last_day
    from (
        select issue_date as day from {{ ref('silver_invoice_events') }}
        union all
        select due_date from {{ ref('silver_invoice_events') }}
        union all
        select coalesce(payment_date, refund_date) from {{ ref('silver_payment_events') }}
    )

),

days as (

    select cast(unnest(generate_series(first_day, last_day, interval 1 day)) as date) as date_day
    from bounds

)

select
    date_day                                            as date_key,
    cast(extract(year from date_day) as integer)        as year,
    cast(extract(quarter from date_day) as integer)     as quarter,
    cast(extract(month from date_day) as integer)       as month,
    strftime(date_day, '%Y-%m')                         as year_month,
    cast(date_trunc('month', date_day) as date)         as month_start,
    cast(extract(isodow from date_day) as integer)      as iso_day_of_week,   -- 1 = Monday ... 7 = Sunday
    extract(isodow from date_day) in (6, 7)             as is_weekend
from days

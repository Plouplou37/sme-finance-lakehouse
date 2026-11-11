{{ config(location='s3://sme-lake/gold/mart_late_payers.parquet') }}

-- Question 4: which customers pay late most often?
-- Grain: one row per customer (business key), with its current name and city.
-- An invoice counts as late if it was settled after its due date, or is still unpaid past it.
-- Only invoices that can already be judged are counted: settled ones, and unpaid ones past their due date.

with judged as (

    select *
    from {{ ref('fct_invoices') }}
    where status = 'paid'
       or (status in ('open', 'partially_paid') and days_late > 0)

)

select
    judged.customer_id,
    dim_customer.name,
    dim_customer.city,
    cast(count(*) as integer)                                                       as judged_invoices,
    cast(count(*) filter (where judged.days_late > 0) as integer)                   as late_invoices,
    cast(100.0 * count(*) filter (where judged.days_late > 0) / count(*) as decimal(5, 1)) as late_share_pct,
    cast(avg(judged.days_late) filter (where judged.days_late > 0) as decimal(10, 1))     as avg_days_late_when_late
from judged
inner join {{ ref('dim_customer') }} as dim_customer
    on dim_customer.customer_id = judged.customer_id
    and dim_customer.is_current
group by all

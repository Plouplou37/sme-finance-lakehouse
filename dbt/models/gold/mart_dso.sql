{{ config(location='s3://sme-lake/gold/mart_dso/mart_dso.parquet') }}

-- Question 3: what is the average payment delay (DSO) of each company?
-- Grain: one row per company, as of the snapshot date. Several views of the same question:
-- - days_to_first_payment: from the issue date to the FIRST payment, partial or not
--   (the customer starts paying); counts partially paid invoices too;
-- - days_to_settle: measured on settled invoices, from the issue date to the settlement date
--   (with partial payments, the date of the payment that completes the invoice), simple and
--   weighted by amount (a large invoice paid late weighs more than a small one);
-- - dso_90_days: the accounting formula, (receivables outstanding / revenue incl. VAT invoiced over
--   the last 90 days) x 90. It also accounts for invoices not settled yet.

with invoices as (

    select * from {{ ref('fct_invoices') }}
    where status != 'cancelled'

)

select
    company_id,
    max(as_of_date)                                                           as as_of_date,
    cast(count(*) filter (where status = 'paid') as integer)                  as settled_invoices,
    cast(avg(first_payment_date - issue_date) as decimal(10, 1))              as avg_days_to_first_payment,
    cast(avg(days_to_settle) as decimal(10, 1))                               as avg_days_to_settle,
    cast(
        sum(days_to_settle * amount_incl_vat) filter (where status = 'paid')
        / sum(amount_incl_vat) filter (where status = 'paid')
        as decimal(10, 1)
    )                                                                         as weighted_days_to_settle,
    cast(sum(amount_outstanding) as decimal(18, 2))                           as receivables_outstanding,
    cast(sum(amount_incl_vat) filter (where issue_date > as_of_date - 90) as decimal(18, 2)) as invoiced_last_90_days,
    cast(
        sum(amount_outstanding)
        / sum(amount_incl_vat) filter (where issue_date > as_of_date - 90) * 90
        as decimal(10, 1)
    )                                                                         as dso_90_days
from invoices
group by company_id

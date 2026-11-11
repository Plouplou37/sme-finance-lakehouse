{{ config(location='s3://sme-lake/gold/mart_monthly_revenue.parquet') }}

-- Question 1: what is the monthly revenue of each company?
-- Grain: one row per company and per month of issue.
-- Revenue = invoiced amounts EXCLUDING VAT, by the month of the issue date (the legal date on the
-- invoice, not the time the event arrived). Cancelled invoices are not revenue.

select
    fct_invoices.company_id,
    dim_date.year_month,
    dim_date.month_start,
    count(*)                              as invoices_issued,
    sum(fct_invoices.amount_excl_vat)     as revenue_excl_vat,
    sum(fct_invoices.amount_incl_vat)     as invoiced_incl_vat
from {{ ref('fct_invoices') }} as fct_invoices
inner join {{ ref('dim_date') }} as dim_date on dim_date.date_key = fct_invoices.issue_date
where fct_invoices.status != 'cancelled'
group by all

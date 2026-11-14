-- Question 1: what is the monthly revenue of each company?
-- Revenue = invoiced amounts excluding VAT, by month of the issue date (the legal date on the invoice).
-- Cancelled invoices are not revenue.
-- Written for Athena (Trino) and DuckDB alike: scripts/check_athena.py runs it on both and compares.
select
    fct_invoices.company_id,
    dim_date.year_month,
    count(*)                                                  as invoices_issued,
    cast(sum(fct_invoices.amount_excl_vat) as decimal(18, 2)) as revenue_excl_vat
from sme_lake_gold.fct_invoices
inner join sme_lake_gold.dim_date
    on dim_date.date_key = fct_invoices.issue_date
where fct_invoices.status <> 'cancelled'
group by fct_invoices.company_id, dim_date.year_month
order by fct_invoices.company_id, dim_date.year_month

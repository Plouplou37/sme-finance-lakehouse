-- Question 3: what is the average payment delay (DSO) of each company?
-- Two views of the same question:
-- - avg_days_to_settle: on settled invoices, days from issue to settlement;
-- - dso_90_days: the accounting formula, receivables outstanding / revenue incl. VAT invoiced over the
--   last 90 days x 90. It also accounts for invoices not settled yet.
-- Portable on purpose: date_diff and "interval '90' day" exist in Trino and DuckDB alike; DuckDB's
-- shortcuts (date - date, date - 90) do not work in Athena.
select
    company_id,
    count(*) filter (where status = 'paid') as settled_invoices,
    cast(avg(date_diff('day', issue_date, settled_date)) filter (where status = 'paid') as decimal(10, 1))
        as avg_days_to_settle,
    cast(sum(amount_outstanding) as decimal(18, 2)) as receivables_outstanding,
    cast(
        sum(amount_outstanding)
        / sum(amount_incl_vat) filter (where issue_date > as_of_date - interval '90' day)
        * 90
        as decimal(10, 1)
    ) as dso_90_days
from sme_lake_gold.fct_invoices
where status <> 'cancelled'
group by company_id
order by company_id

-- Question 2: which invoices are unpaid, and how late are they?
-- Unpaid = open or partially paid. Unpaid does not mean late: days_late is 0 until the due date.
-- The customer's name is its current one (the current version of the SCD2 dimension).
select
    fct_invoices.invoice_id,
    fct_invoices.company_id,
    dim_customer.name as customer_name,
    fct_invoices.due_date,
    fct_invoices.days_late,
    fct_invoices.amount_outstanding
from sme_lake_gold.fct_invoices
inner join sme_lake_gold.dim_customer
    on
        fct_invoices.customer_id = dim_customer.customer_id
        and dim_customer.is_current
where fct_invoices.status in ('open', 'partially_paid')
order by fct_invoices.days_late desc, fct_invoices.invoice_id asc

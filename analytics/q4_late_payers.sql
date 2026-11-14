-- Question 4: which customers pay late most often?
-- An invoice counts as late if it was settled after its due date, or is still unpaid past it. Only invoices
-- that can already be judged are counted: settled ones, and unpaid ones past their due date.
with judged as (
    select customer_id, days_late
    from sme_lake_gold.fct_invoices
    where status = 'paid'
        or (status in ('open', 'partially_paid') and days_late > 0)
)

select
    judged.customer_id,
    dim_customer.name,
    dim_customer.city,
    count(*)                                     as judged_invoices,
    count(*) filter (where judged.days_late > 0) as late_invoices,
    cast(100.0 * count(*) filter (where judged.days_late > 0) / count(*) as decimal(5, 1)) as late_share_pct
from judged
inner join sme_lake_gold.dim_customer
    on dim_customer.customer_id = judged.customer_id
    and dim_customer.is_current
group by judged.customer_id, dim_customer.name, dim_customer.city
order by late_share_pct desc, late_invoices desc, judged.customer_id

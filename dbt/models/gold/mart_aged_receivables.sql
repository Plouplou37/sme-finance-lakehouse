-- Question 2: which invoices are unpaid, and how late are they?
-- Grain: one row per company and per ageing bucket, as of the snapshot date.
-- Unpaid does not mean late: an open invoice whose due date has not passed is "not yet due".
-- The invoice-level detail is in fct_invoices (status open or partially_paid, days_late).

select
    company_id,
    as_of_date,
    case
        when days_late = 0 then '0 - not yet due'
        when days_late <= 30 then '1 - 1 to 30 days late'
        when days_late <= 60 then '2 - 31 to 60 days late'
        when days_late <= 90 then '3 - 61 to 90 days late'
        else '4 - more than 90 days late'
    end                          as ageing_bucket,
    cast(count(*) as integer)                           as unpaid_invoices,
    cast(sum(amount_outstanding) as decimal(18, 2))     as amount_outstanding
from {{ ref('fct_invoices') }}
where status in ('open', 'partially_paid')
group by all

-- Gold must say the same thing as silver, computed directly and independently from the silver tables.
-- Returns one row per measure that does not match (0 rows = pass).

with latest_invoice_versions as (

    select invoice_id, amount_excl_vat, amount_incl_vat
    from {{ ref('silver_invoice_events') }}
    where event_type in ('invoice_issued', 'invoice_updated')
    qualify row_number() over (partition by invoice_id order by event_time desc) = 1

),

cancelled as (

    select distinct invoice_id
    from {{ ref('silver_invoice_events') }}
    where event_type = 'invoice_cancelled'

),

checks as (

    select
        'number of invoices' as measure,
        (select count(*) from latest_invoice_versions) as silver_value,
        (select count(*) from {{ ref('fct_invoices') }}) as gold_value

    union all

    select
        'total invoiced incl. VAT',
        (select sum(amount_incl_vat) from latest_invoice_versions),
        (select sum(amount_incl_vat) from {{ ref('fct_invoices') }})

    union all

    select
        'revenue excl. VAT (not cancelled)',
        (select sum(amount_excl_vat) from latest_invoice_versions
         where invoice_id not in (select invoice_id from cancelled)),
        (select sum(revenue_excl_vat) from {{ ref('mart_monthly_revenue') }})

    union all

    select
        'net amount received',
        (select sum(case when event_type = 'payment_received' then amount else -amount end)
         from {{ ref('silver_payment_events') }}),
        (select sum(amount) from {{ ref('fct_payments') }})

)

select *
from checks
where silver_value != gold_value

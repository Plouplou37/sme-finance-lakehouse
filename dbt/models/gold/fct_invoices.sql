{{ config(location='s3://sme-lake/gold/fct_invoices.parquet') }}

-- Grain: one row per invoice (accumulating snapshot). The row gathers the milestones of the invoice
-- (issued, due, settled, cancelled) and its measures, as of the last event received (as_of_date).
-- Revenue uses amount_excl_vat; whether the invoice is paid uses amount_incl_vat (what the customer owes).

with invoice_events as (

    select * from {{ ref('silver_invoice_events') }}

),

issued as (

    select invoice_id, company_id, customer_id, issue_date, event_time as issued_at
    from invoice_events
    where event_type = 'invoice_issued'

),

latest_version as (

    -- Amounts and due date of the latest version: invoice_issued, possibly followed by invoice_updated.
    select invoice_id, due_date, currency, amount_excl_vat, vat_rate, vat_amount, amount_incl_vat
    from invoice_events
    where event_type in ('invoice_issued', 'invoice_updated')
    qualify row_number() over (partition by invoice_id order by event_time desc, event_id desc) = 1

),

cancellations as (

    select invoice_id, cast(event_time as date) as cancelled_date, cancellation_reason
    from invoice_events
    where event_type = 'invoice_cancelled'
    qualify row_number() over (partition by invoice_id order by event_time) = 1

),

movements as (

    -- Money in (+) and out (-) for each invoice.
    select invoice_id, payment_date as movement_date, amount as signed_amount, 'payment' as kind, event_time
    from {{ ref('silver_payment_events') }}
    where event_type = 'payment_received'
    union all
    select invoice_id, refund_date, -amount, 'refund', event_time
    from {{ ref('silver_payment_events') }}
    where event_type = 'payment_refunded'

),

paid as (

    select
        invoice_id,
        sum(signed_amount)                                   as amount_paid,
        count(*) filter (where kind = 'payment')             as payment_count,
        min(movement_date) filter (where kind = 'payment')   as first_payment_date
    from movements
    group by invoice_id

),

settlement as (

    -- The settlement date: the first day the cumulated payments reach the amount due.
    -- With partial payments, it is the date of the payment that completes the invoice.
    select running.invoice_id, min(running.movement_date) as settled_date
    from (
        select
            movements.*,
            sum(signed_amount) over (
                partition by invoice_id
                order by movement_date, event_time
                rows between unbounded preceding and current row
            ) as paid_so_far
        from movements
    ) as running
    inner join latest_version using (invoice_id)
    where running.paid_so_far >= latest_version.amount_incl_vat
    group by running.invoice_id

),

as_of as (

    -- The snapshot date: the day of the last event received. "Late" and "outstanding" are measured then.
    select max(cast(event_time as date)) as as_of_date
    from (
        select event_time from invoice_events
        union all
        select event_time from {{ ref('silver_payment_events') }}
    )

),

customer_version as (

    -- Point-in-time join: the version of the customer that was valid when the invoice was issued (SCD2).
    select issued.invoice_id, dim_customer.customer_key
    from issued
    inner join {{ ref('dim_customer') }} as dim_customer
        on dim_customer.customer_id = issued.customer_id
        and issued.issued_at >= dim_customer.valid_from
        and (dim_customer.valid_to is null or issued.issued_at < dim_customer.valid_to)

)

select
    issued.invoice_id,
    customer_version.customer_key,
    dim_company.company_key,
    issued.customer_id,
    issued.company_id,
    issued.issue_date,
    latest_version.due_date,
    settlement.settled_date,
    cancellations.cancelled_date,
    cancellations.cancellation_reason,
    latest_version.currency,
    latest_version.amount_excl_vat,
    latest_version.vat_amount,
    latest_version.amount_incl_vat,
    coalesce(paid.amount_paid, 0)                                          as amount_paid,
    case
        when cancellations.invoice_id is not null then 0
        else latest_version.amount_incl_vat - coalesce(paid.amount_paid, 0)
    end                                                                    as amount_outstanding,
    coalesce(paid.payment_count, 0)                                        as payment_count,
    paid.first_payment_date,
    case
        when cancellations.invoice_id is not null then 'cancelled'
        when settlement.settled_date is not null then 'paid'
        when coalesce(paid.amount_paid, 0) > 0 then 'partially_paid'
        else 'open'
    end                                                                    as status,
    case
        when cancellations.invoice_id is null and settlement.settled_date is not null
            then settlement.settled_date - issued.issue_date
    end                                                                    as days_to_settle,
    case
        when cancellations.invoice_id is not null then null
        when settlement.settled_date is not null then greatest(settlement.settled_date - latest_version.due_date, 0)
        else greatest(as_of.as_of_date - latest_version.due_date, 0)
    end                                                                    as days_late,
    as_of.as_of_date
from issued
inner join latest_version using (invoice_id)
left join cancellations using (invoice_id)
left join paid using (invoice_id)
left join settlement using (invoice_id)
left join customer_version using (invoice_id)
left join {{ ref('dim_company') }} as dim_company on dim_company.company_id = issued.company_id
cross join as_of

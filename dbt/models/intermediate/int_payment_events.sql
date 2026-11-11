-- Payment events (payment_received, payment_refunded), deduplicated, with the fields of the payment parsed
-- and typed, and a rejection reason (NULL = valid).
-- Schema versions 1 and 2 side by side: payment_method only exists in version 2, so it is NULL for
-- version 1 events. Same principle as int_invoice_events: one reason per event, the first rule broken.

with payment_events as (

    select
        event_id,
        event_type,
        event_time,
        schema_version,
        company_id,
        {{ json_text('raw_json', 'invoice_id') }}                                         as invoice_id,
        {{ json_text('raw_json', 'payment_id') }}                                         as payment_id,
        {{ safe_cast_to(json_text('raw_json', 'payment_date'), 'date') }}                 as payment_date,
        {{ json_text('raw_json', 'payment_method') }}                                     as payment_method,
        {{ json_text('raw_json', 'refund_id') }}                                          as refund_id,
        {{ safe_cast_to(json_text('raw_json', 'refund_date'), 'date') }}                  as refund_date,
        {{ safe_cast_to(json_text('raw_json', 'amount'), 'decimal(18, 2)') }}             as amount,
        {{ json_text('raw_json', 'currency') }}                                           as currency,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        ingested_at
    from {{ ref('stg_events_deduplicated') }}
    where topic = 'finance.payments.v1'

),

valid_invoices as (

    select distinct invoice_id
    from {{ ref('int_invoice_events') }}
    where event_type = 'invoice_issued' and rejection_reason is null

),

amount_due as (

    -- What each valid invoice asks for: the amount incl. VAT of its LATEST valid version
    -- (invoice_issued, possibly followed by an invoice_updated).
    select invoice_id, amount_incl_vat
    from {{ ref('int_invoice_events') }}
    where event_type in ('invoice_issued', 'invoice_updated') and rejection_reason is null
    qualify row_number() over (partition by invoice_id order by event_time desc) = 1

),

received_checked as (

    -- The rules that look at one payment alone.
    select
        payment_events.*,
        case
            when amount is null or amount <= 0
                then 'non_positive_amount'
            when currency is distinct from 'EUR'
                then 'unknown_currency'
            when not exists (
                select 1 from valid_invoices
                where valid_invoices.invoice_id = payment_events.invoice_id
            )
                then 'unknown_reference'
        end as single_payment_reason
    from payment_events
    where event_type = 'payment_received'

),

received_with_total as (

    -- Overpayment needs the history of the invoice: the total of its valid payments, in date order,
    -- up to and including this one. A payment that makes this total exceed the amount due is rejected.
    select
        received_checked.*,
        amount_due.amount_incl_vat as amount_due,
        sum(case when single_payment_reason is null then amount else 0 end) over (
            partition by received_checked.invoice_id
            order by payment_date, event_time, payment_id
            rows between unbounded preceding and current row
        ) as paid_so_far
    from received_checked
    left join amount_due using (invoice_id)

),

received as (

    select
        * exclude (single_payment_reason, amount_due, paid_so_far),
        case
            when single_payment_reason is not null then single_payment_reason
            when paid_so_far > amount_due then 'overpayment'
        end as rejection_reason
    from received_with_total

),

valid_payments as (

    select payment_id
    from received
    where rejection_reason is null

),

refunded as (

    select
        payment_events.*,
        case
            when amount is null or amount <= 0
                then 'non_positive_amount'
            when currency is distinct from 'EUR'
                then 'unknown_currency'
            when not exists (
                select 1 from valid_payments
                where valid_payments.payment_id = payment_events.payment_id
            )
                then 'unknown_reference'
        end as rejection_reason
    from payment_events
    where event_type = 'payment_refunded'

)

select * from received
union all
select * from refunded

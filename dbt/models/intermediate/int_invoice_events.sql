-- Invoice events (invoice_issued, invoice_updated, invoice_cancelled), deduplicated, with the fields of the
-- invoice parsed and typed, and a rejection reason (NULL = valid).
-- One reason per event: the first rule broken, in the order of each CASE below. Reasons use the anomaly
-- names of the generator's manifest, so the quarantine can be reconciled with it.
-- Amounts are decimal strings in the contract: cast to DECIMAL, never to a float.

with invoice_events as (

    select
        event_id,
        event_type,
        event_time,
        schema_version,
        company_id,
        {{ json_text('raw_json', 'invoice_id') }}                                         as invoice_id,
        {{ json_text('raw_json', 'customer_id') }}                                        as customer_id,
        {{ safe_cast_to(json_text('raw_json', 'issue_date'), 'date') }}                   as issue_date,
        {{ safe_cast_to(json_text('raw_json', 'due_date'), 'date') }}                     as due_date,
        {{ json_text('raw_json', 'currency') }}                                           as currency,
        {{ safe_cast_to(json_text('raw_json', 'amount_excl_vat'), 'decimal(18, 2)') }}    as amount_excl_vat,
        {{ safe_cast_to(json_text('raw_json', 'vat_rate'), 'decimal(6, 4)') }}            as vat_rate,
        {{ safe_cast_to(json_text('raw_json', 'vat_amount'), 'decimal(18, 2)') }}         as vat_amount,
        {{ safe_cast_to(json_text('raw_json', 'amount_incl_vat'), 'decimal(18, 2)') }}    as amount_incl_vat,
        {{ json_text('raw_json', 'cancellation_reason') }}                                as cancellation_reason,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        ingested_at
    from {{ ref('stg_events_deduplicated') }}
    where topic = 'finance.invoices.v1'

),

known_customers as (

    select distinct customer_id
    from {{ ref('int_customer_events') }}
    where rejection_reason is null

),

issued_or_updated as (

    select
        invoice_events.*,
        case
            when amount_excl_vat is null or amount_excl_vat <= 0
              or amount_incl_vat is null or amount_incl_vat <= 0
                then 'non_positive_amount'
            -- "is distinct from" and not "<>": with a NULL currency, "<>" gives NULL, and the event would pass
            when currency is distinct from 'EUR'
                then 'unknown_currency'
            -- "not exists" and not "not in": "not in" a list holding a NULL is never true
            when not exists (
                select 1 from known_customers
                where known_customers.customer_id = invoice_events.customer_id
            )
                then 'unknown_reference'
        end as rejection_reason
    from invoice_events
    where event_type in ('invoice_issued', 'invoice_updated')

),

valid_invoices as (

    select distinct invoice_id
    from issued_or_updated
    where event_type = 'invoice_issued' and rejection_reason is null

),

cancelled as (

    select
        invoice_events.*,
        case
            when not exists (
                select 1 from valid_invoices
                where valid_invoices.invoice_id = invoice_events.invoice_id
            )
                then 'unknown_reference'
        end as rejection_reason
    from invoice_events
    where event_type = 'invoice_cancelled'

)

select * from issued_or_updated
union all
select * from cancelled

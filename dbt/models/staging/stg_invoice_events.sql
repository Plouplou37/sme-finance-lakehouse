-- Invoice events (topic finance.invoices.v1): invoice_issued, invoice_updated, invoice_cancelled.
-- Parses the fields specific to invoices out of raw_json and types them (event contract).
-- Amounts are decimal strings in the contract: cast to DECIMAL, never to a float (0.1 + 0.2 != 0.3).
-- A value that cannot be converted becomes NULL (safe cast); silver will reject it. No rule here.
-- A view: recomputed when read, from the stg_events table (no new read of S3).

with events as (

    select * from {{ ref('stg_events') }}
    where topic = 'finance.invoices.v1'

)

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
from events

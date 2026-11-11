-- Payment events (topic finance.payments.v1): payment_received and payment_refunded.
-- Parses the fields specific to payments out of raw_json and types them (event contract).
-- Schema versions 1 and 2 side by side: payment_method only exists in version 2, so it is NULL
-- for version 1 events (json_text returns NULL for a missing field) instead of breaking the parse.
-- No rule here: silver judges amounts, currencies and references.

with events as (

    select * from {{ ref('stg_events') }}
    where topic = 'finance.payments.v1'

)

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
from events

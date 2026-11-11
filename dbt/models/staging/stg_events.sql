{{ config(materialized='table') }}

-- One row per bronze row, that is per Kafka message.
-- Parses the ENVELOPE only: the 5 fields every event has (event contract). The fields specific to each
-- event type stay in raw_json and are parsed by stg_customer_events, stg_invoice_events and stg_payment_events.
-- A table (not a view): S3 is read once per run, then every reader uses this table.
-- No business rule here: invalid or duplicate events are kept as they are, and judged in silver.

with bronze as (

    select * from {{ source('bronze', 'events') }}

)

select
    {{ json_text('raw_json', 'event_id') }}                                          as event_id,
    {{ json_text('raw_json', 'event_type') }}                                        as event_type,
    {{ safe_cast_to(json_text('raw_json', 'event_time'), 'timestamptz') }}           as event_time,
    {{ safe_cast_to(json_text('raw_json', 'schema_version'), 'integer') }}           as schema_version,
    {{ json_text('raw_json', 'company_id') }}                                        as company_id,
    raw_json,
    topic,
    kafka_key,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingested_at,
    ingestion_date
from bronze

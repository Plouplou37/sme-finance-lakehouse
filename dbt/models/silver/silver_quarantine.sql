{{ config(location='s3://sme-lake/silver/silver_quarantine.parquet') }}

-- Rejected events: never deleted, kept with their reason and their raw JSON, to audit them, count them by
-- reason (reconciliation with the generator's manifest), warn the source team, and replay them once a rule
-- is fixed. Reasons use the anomaly names of the manifest.

with rejected as (

    select event_id, event_type, rejection_reason from {{ ref('int_customer_events') }}
    where rejection_reason is not null
    union all
    select event_id, event_type, rejection_reason from {{ ref('int_invoice_events') }}
    where rejection_reason is not null
    union all
    select event_id, event_type, rejection_reason from {{ ref('int_payment_events') }}
    where rejection_reason is not null

)

select
    rejected.event_id,
    rejected.event_type,
    rejected.rejection_reason,
    deduplicated.topic,
    deduplicated.company_id,
    deduplicated.event_time,
    deduplicated.kafka_partition,
    deduplicated.kafka_offset,
    deduplicated.ingested_at,
    deduplicated.raw_json
from rejected
inner join {{ ref('stg_events_deduplicated') }} as deduplicated using (event_id)

union all

-- Messages whose event_id cannot be read: they could not even be deduplicated.
select
    null as event_id,
    event_type,
    'unreadable_event' as rejection_reason,
    topic,
    company_id,
    event_time,
    kafka_partition,
    kafka_offset,
    ingested_at,
    raw_json
from {{ ref('stg_events') }}
where event_id is null

{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}

-- One row per event_id. A duplicate is the same event received twice (at-least-once delivery:
-- a producer resend or a consumer re-read), so it has the same event_id: keep the first copy that
-- reached Kafka. Technical cleaning, no business rule: that is why it belongs to staging.
-- Events without a readable event_id are left out here and quarantined in silver.
--
-- Incremental: the first run reads all of stg_events; later runs only read the rows ingested since
-- the last run, minus a 3-day margin. Thanks to unique_key = event_id, a row read again (because of the
-- margin, or a duplicate arriving in a later run) REPLACES the stored row instead of adding a copy.

with new_rows as (

    select * from {{ ref('stg_events') }}
    where event_id is not null
    {% if is_incremental() %}
        -- {{ this }} is this model's own table, as stored by the previous run
        and ingested_at > (select max(ingested_at) from {{ this }}) - interval 3 day
    {% endif %}

),

ranked as (

    select
        *,
        row_number() over (
            partition by event_id
            order by kafka_timestamp, topic, kafka_partition, kafka_offset
        ) as copy_number
    from new_rows

)

select
    event_id,
    event_type,
    event_time,
    schema_version,
    company_id,
    raw_json,
    topic,
    kafka_key,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingested_at,
    ingestion_date
from ranked
where copy_number = 1

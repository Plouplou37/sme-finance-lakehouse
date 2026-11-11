-- Customer events (customer_created, customer_updated), deduplicated, with the fields of the customer
-- parsed and typed, and a rejection reason (NULL = valid).
-- Both events carry the full state of the customer (event contract): each one is a version, which the
-- SCD2 dimension of M6 will turn into a row of history.
-- Postal code and phone stay text: as numbers, "01000" would become 1000 and "+33…" would lose its "+".

with customer_events as (

    select
        event_id,
        event_type,
        event_time,
        schema_version,
        company_id,
        {{ json_text('raw_json', 'customer_id') }}                                    as customer_id,
        {{ json_text('raw_json', 'customer_type') }}                                  as customer_type,
        {{ json_text('raw_json', 'name') }}                                           as name,
        {{ json_text('raw_json', 'email') }}                                          as email,
        {{ json_text('raw_json', 'phone') }}                                          as phone,
        {{ json_text('raw_json', 'street') }}                                         as street,
        {{ json_text('raw_json', 'postal_code') }}                                    as postal_code,
        {{ json_text('raw_json', 'city') }}                                           as city,
        {{ json_text('raw_json', 'country') }}                                        as country,
        {{ safe_cast_to(json_text('raw_json', 'payment_terms_days'), 'integer') }}    as payment_terms_days,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        ingested_at
    from {{ ref('stg_events_deduplicated') }}
    where topic = 'finance.customers.v1'

)

select
    *,
    -- The generator injects no customer anomaly; the only rule is the one every event needs.
    case
        when customer_id is null then 'unknown_reference'
    end as rejection_reason
from customer_events

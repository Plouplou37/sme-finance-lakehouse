-- Customer events (topic finance.customers.v1): customer_created and customer_updated.
-- Both carry the full state of the customer (event contract): each event is one version,
-- which the SCD2 dimension of M6 will turn into a row of history.
-- Postal code and phone stay text: as numbers, "01000" would become 1000 and "+33…" would lose its "+".

with events as (

    select * from {{ ref('stg_events') }}
    where topic = 'finance.customers.v1'

)

select
    event_id,
    event_type,
    event_time,
    schema_version,
    company_id,
    {{ json_text('raw_json', 'customer_id') }}                                        as customer_id,
    {{ json_text('raw_json', 'customer_type') }}                                      as customer_type,
    {{ json_text('raw_json', 'name') }}                                               as name,
    {{ json_text('raw_json', 'email') }}                                              as email,
    {{ json_text('raw_json', 'phone') }}                                              as phone,
    {{ json_text('raw_json', 'street') }}                                             as street,
    {{ json_text('raw_json', 'postal_code') }}                                        as postal_code,
    {{ json_text('raw_json', 'city') }}                                               as city,
    {{ json_text('raw_json', 'country') }}                                            as country,
    {{ safe_cast_to(json_text('raw_json', 'payment_terms_days'), 'integer') }}        as payment_terms_days,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingested_at
from events

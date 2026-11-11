-- Grain: one row per check of the platform against the generator's manifest.
-- The manifest is an independent truth: what the generator produced and which anomalies it injected,
-- type by type. Each check compares it with what the platform found. M8 will show this table in the
-- report of each run.
--   total_events          Kafka -> bronze -> dbt lost nothing: bronze rows = events produced
--   duplicate             duplicates removed by deduplication = duplicates injected
--   non_positive_amount…  quarantine by reason = anomalies injected, type by type
--   late_arrival          not checked: the producer published every file at once, so Kafka's timestamp
--                         is the publication time, and an arrival delay cannot be measured in bronze.

with manifest as (

    select content from {{ source('generator', 'manifest') }}

),

expected as (

    select 'total_events' as check_name,
           {{ safe_cast_to(json_text('content', 'events.total'), 'integer') }} as expected
    from manifest
    {% for anomaly in ['duplicate', 'late_arrival', 'non_positive_amount', 'unknown_currency', 'unknown_reference', 'overpayment'] %}
    union all
    -- A type with no anomaly is absent from the manifest: count it as 0.
    select '{{ anomaly }}',
           coalesce({{ safe_cast_to(json_text('content', 'anomalies.by_type.' ~ anomaly), 'integer') }}, 0)
    from manifest
    {% endfor %}

),

detected as (

    select 'total_events' as check_name, cast(count(*) as integer) as detected
    from {{ ref('stg_events') }}

    union all

    select 'duplicate', cast(count(*) - count(distinct event_id) as integer)
    from {{ ref('stg_events') }}
    where event_id is not null

    union all

    select rejection_reason, cast(count(*) as integer)
    from {{ ref('silver_quarantine') }}
    group by rejection_reason

)

select
    expected.check_name,
    expected.expected,
    case when expected.check_name = 'late_arrival' then null else coalesce(detected.detected, 0) end as detected,
    case
        when expected.check_name = 'late_arrival' then 'not_checked'
        when expected.expected = coalesce(detected.detected, 0) then 'match'
        else 'mismatch'
    end as status
from expected
left join detected using (check_name)

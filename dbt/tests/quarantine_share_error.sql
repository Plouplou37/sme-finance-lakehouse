{{ config(severity='error') }}

-- Blocking, and placed BEFORE publication (Write-Audit-Publish): it reads the intermediate models only. More
-- than 5 % of events rejected suggests something broken (a schema change, a wrong rule) rather than a few bad
-- events.
-- A single `dbt build` does NOT stop the publication when it fails: dbt only skips the models whose ancestors
-- include every model this test reads, and dim_customer (customers only) was published anyway (seen in M8).
-- The barrier is the Airflow DAG: dbt_audit runs this test, dbt_publish (silver, gold) only starts if it passes.
-- The threshold is a dbt variable, so the M8 demo can lower it to force a failure:
--   dbt build --vars '{"quarantine_error_share": 0.005}'
-- Returns one row when the share is above the threshold (0 rows = pass).

with rejected as (

    select event_id from {{ ref('int_customer_events') }} where rejection_reason is not null
    union all
    select event_id from {{ ref('int_invoice_events') }} where rejection_reason is not null
    union all
    select event_id from {{ ref('int_payment_events') }} where rejection_reason is not null

),

counts as (

    select
        (select count(*) from rejected) as rejected,
        (select count(*) from {{ ref('stg_events_deduplicated') }}) as events

)

select rejected, events, round(100.0 * rejected / events, 2) as rejected_share_pct
from counts
where rejected > {{ var('quarantine_error_share', 0.05) }} * events

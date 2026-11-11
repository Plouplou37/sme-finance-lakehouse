{{ config(severity='error') }}

-- Blocking: more than 5 % of the events in quarantine suggests something broken (a schema change, a
-- wrong rule) rather than a few bad events. Better stop than publish a gold layer missing a large part
-- of the data. Returns one row when the share is above the threshold (0 rows = pass).

with counts as (

    select
        (select count(*) from {{ ref('silver_quarantine') }}) as quarantined,
        (select count(distinct event_id) from {{ ref('stg_events') }}) as events

)

select quarantined, events, round(100.0 * quarantined / events, 2) as quarantine_share_pct
from counts
where quarantined > 0.05 * events

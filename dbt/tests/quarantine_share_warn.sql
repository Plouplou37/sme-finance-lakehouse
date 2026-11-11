{{ config(severity='warn') }}

-- Alerting, not blocking: more than 1 % of the events in quarantine deserves a look (a source sending
-- worse data than usual?), but the valid events are still right, so the run goes on.
-- Returns one row when the share is above the threshold (0 rows = pass).

with counts as (

    select
        (select count(*) from {{ ref('silver_quarantine') }}) as quarantined,
        (select count(distinct event_id) from {{ ref('stg_events') }}) as events

)

select quarantined, events, round(100.0 * quarantined / events, 2) as quarantine_share_pct
from counts
where quarantined > 0.01 * events

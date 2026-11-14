{{ config(severity='warn') }}

-- Alerting, not blocking: more than 1 % of the events rejected deserves a look (a source sending worse data
-- than usual?), but the valid events are still right, so the run goes on.
-- Reads the intermediate models, like its blocking twin quarantine_share_error.
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
where rejected > {{ var('quarantine_warn_share', 0.01) }} * events

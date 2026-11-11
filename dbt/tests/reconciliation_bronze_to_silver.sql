-- Every bronze message ends up in exactly one place: silver, quarantine, or counted as a duplicate.
-- A dbt test passes when it returns no row: this query returns one row only if the counts do not add up.

with counts as (

    select
        (select count(*) from {{ ref('stg_events') }}) as bronze_rows,
        (select count(*) from {{ ref('silver_customer_events') }})
          + (select count(*) from {{ ref('silver_invoice_events') }})
          + (select count(*) from {{ ref('silver_payment_events') }}) as silver_rows,
        (select count(*) from {{ ref('silver_quarantine') }}) as quarantine_rows,
        (select count(*) - count(distinct event_id) from {{ ref('stg_events') }} where event_id is not null) as duplicates

)

select *
from counts
where bronze_rows != silver_rows + quarantine_rows + duplicates

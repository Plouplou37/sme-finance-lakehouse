-- SCD2 checks on dim_customer. Returns one row per problem found (0 rows = pass):
-- 1. every change of a customer (customer_updated) created a new version starting at that change;
-- 2. every customer has exactly one current version.

select 'missing version for an update' as problem, events.customer_id, events.event_time
from {{ ref('silver_customer_events') }} as events
where events.event_type = 'customer_updated'
  and not exists (
      select 1 from {{ ref('dim_customer') }} as dim_customer
      where dim_customer.customer_id = events.customer_id
        and dim_customer.valid_from = events.event_time
  )

union all

select 'not exactly one current version' as problem, customer_id, null as event_time
from {{ ref('dim_customer') }}
group by customer_id
having count(*) filter (where is_current) != 1

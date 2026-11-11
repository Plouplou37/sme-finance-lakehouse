-- Valid customer events, deduplicated and typed: one row per customer version (created or updated).
-- M6 turns them into the SCD2 dimension dim_customer.

select * exclude (rejection_reason)
from {{ ref('int_customer_events') }}
where rejection_reason is null

{{ config(location='s3://sme-lake/silver/silver_payment_events.parquet') }}

-- Valid payment events, deduplicated and typed: payments received (schema v1 and v2) and refunds.

select * exclude (rejection_reason)
from {{ ref('int_payment_events') }}
where rejection_reason is null

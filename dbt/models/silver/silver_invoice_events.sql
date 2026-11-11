{{ config(location='s3://sme-lake/silver/silver_invoice_events.parquet') }}

-- Valid invoice events, deduplicated and typed: issued, updated and cancelled.
-- The grain stays the event, so the history is kept; M6 builds the current state of each invoice.

select * exclude (rejection_reason)
from {{ ref('int_invoice_events') }}
where rejection_reason is null

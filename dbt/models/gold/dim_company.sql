{{ config(location='s3://sme-lake/gold/dim_company/dim_company.parquet') }}

-- Grain: one row per company (a small business using the accounting software).
-- A thin dimension: the event contract sends no company attribute (name, sector...), only its ID.
-- It still gives the facts one place to join, ready for attributes if the application sends them one day.

with company_events as (

    select company_id, event_time from {{ ref('silver_customer_events') }}
    union all
    select company_id, event_time from {{ ref('silver_invoice_events') }}

)

select
    {{ surrogate_key(['company_id']) }} as company_key,
    company_id,
    min(event_time) as first_seen_at
from company_events
group by company_id

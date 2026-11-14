{{ config(location='s3://sme-lake/gold/dim_customer/dim_customer.parquet') }}

-- Grain: one row per VERSION of a customer (SCD2). A customer who moved once has two rows.
-- Built from the history already kept in silver: every customer_created or customer_updated event
-- carries the full state of the customer, so each event is a version. No dbt snapshot needed:
-- a snapshot captures the state of a table at each run, for sources that overwrite their rows;
-- here the source is an event log, and it gives the exact time of each change.

with versions as (

    select
        customer_id,
        company_id,
        customer_type,
        name,
        email,
        phone,
        street,
        postal_code,
        city,
        country,
        payment_terms_days,
        event_time as valid_from,
        -- A version is valid until the next version of the same customer starts.
        lead(event_time) over (partition by customer_id order by event_time, event_id) as valid_to
    from {{ ref('silver_customer_events') }}

)

select
    {{ surrogate_key(['customer_id', 'valid_from']) }} as customer_key,   -- one key per version
    customer_id,                                                          -- business key, shared by all versions
    company_id,
    customer_type,
    name,
    email,
    phone,
    street,
    postal_code,
    city,
    country,
    payment_terms_days,
    valid_from,
    valid_to,                                                             -- NULL: still the current version
    valid_to is null as is_current
from versions

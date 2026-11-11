{{ config(location='s3://sme-lake/gold/fct_payments.parquet') }}

-- Grain: one row per money movement on an invoice: a payment received (amount > 0) or a refund
-- (amount < 0). Not one row per invoice: an invoice can receive several partial payments, and each
-- one has its own date, amount and method. A fine grain can always be summed up, never split back.

with movements as (

    select
        event_id,
        'payment'          as movement_type,
        payment_id         as movement_id,
        payment_id,
        invoice_id,
        payment_date       as movement_date,
        amount             as signed_amount,
        payment_method,                          -- NULL for schema version 1 payments
        schema_version
    from {{ ref('silver_payment_events') }}
    where event_type = 'payment_received'

    union all

    select
        event_id,
        'refund',
        refund_id,
        payment_id,
        invoice_id,
        refund_date,
        -amount,
        null,
        schema_version
    from {{ ref('silver_payment_events') }}
    where event_type = 'payment_refunded'

)

select
    movements.movement_id,
    movements.movement_type,
    movements.payment_id,
    movements.invoice_id,
    fct_invoices.customer_key,                  -- the customer version of the invoice (SCD2)
    fct_invoices.company_key,
    fct_invoices.customer_id,
    fct_invoices.company_id,
    movements.movement_date,
    fct_invoices.due_date,
    movements.signed_amount                     as amount,
    movements.payment_method,
    movements.schema_version,
    cast(
        case
            when movements.movement_type = 'payment' then movements.movement_date - fct_invoices.due_date
        end
        as integer
    )                                           as days_after_due,   -- negative = paid before the due date
    movements.event_id
from movements
inner join {{ ref('fct_invoices') }} as fct_invoices using (invoice_id)

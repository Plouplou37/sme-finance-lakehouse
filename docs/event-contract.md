# Event contract

The agreement between the invoicing application and the data platform: which events are sent,
with which fields, in which format. In this project the application is simulated by the generator
(M2), and the producer (M3) publishes its events to Kafka.

Scenario: small and medium-sized companies (`company`) use a fictional accounting software.
Each company issues invoices to its own customers (`customer`), who pay them, sometimes late
and sometimes in several parts.

## Conventions

| Topic | Rule |
|---|---|
| Format | JSON, one event per Kafka message (one event per line in the generator's files) |
| Field names | English, `snake_case` |
| Amounts | Decimal **strings** with 2 decimals (`"1200.00"`), never JSON numbers: binary floats cannot store most cents exactly (`0.1 + 0.2 = 0.30000000000000004`) |
| VAT rates | Decimal strings (`"0.20"`, `"0.055"`) |
| Dates | ISO 8601 date: `"2026-03-02"` |
| Timestamps | ISO 8601 in UTC: `"2026-03-02T09:15:00Z"` |
| Identifiers | Strings. `event_id` is a UUID; business IDs have a prefix (`cmp-`, `cus-`, `inv-`, `pay-`, `ref-`) |
| Codes | Currency: ISO 4217 (`"EUR"`). Country: ISO 3166-1 alpha-2 (`"FR"`) |
| Updates | An `*_updated` event carries the **full current state**, not only the changed fields |

Simplifications, on purpose:
- One VAT rate per invoice, no invoice lines: the analysts' questions are at invoice level.
- Every amount is in euros. `currency` is still sent, and any code other than `"EUR"` is invalid.
- In France an issued invoice cannot be modified (a credit note is issued instead). Here,
  `invoice_updated` stands for a corrected or renegotiated invoice.
- All data is fictional. E-mail addresses use the reserved `.example` domain (RFC 2606).

## Topics

| Topic | Event types | Kafka key |
|---|---|---|
| `finance.customers.v1` | `customer_created`, `customer_updated` | `customer_id` |
| `finance.invoices.v1` | `invoice_issued`, `invoice_updated`, `invoice_cancelled` | `invoice_id` |
| `finance.payments.v1` | `payment_received`, `payment_refunded` | `invoice_id` |

All events about one invoice share its key, so Kafka keeps them in order.

## Envelope: fields in every event

| Field | Type | Example | Meaning |
|---|---|---|---|
| `event_id` | string (UUID) | `"3f6c1e2a-9b4d-4c1e-8a7f-2d5e6b7c8a90"` | Unique ID of the event, created once when the event happens. A message delivered twice keeps the same `event_id`: this is how duplicates are removed |
| `event_type` | string | `"invoice_issued"` | Which event of the topic this is |
| `event_time` | string (timestamp, UTC) | `"2026-03-02T09:15:00Z"` | When the action happened in the application. Not a business date: an invoice dated 31 March can be entered on 2 April |
| `schema_version` | integer | `1` | Version of this contract the event follows, so consumers know how to read it |
| `company_id` | string | `"cmp-0001"` | The company (software user) the event belongs to |

## Customer events

`customer_created` and `customer_updated` have the same fields (full state).

| Field | Type | Example | Meaning | Personal data |
|---|---|---|---|---|
| `customer_id` | string | `"cus-0042"` | Customer of the company | |
| `customer_type` | string: `company` or `individual` | `"company"` | For an individual, identity and contact fields are personal data | |
| `name` | string | `"Atelier Demo 42"` | Company name, or the person's name | yes |
| `email` | string | `"contact@atelier-demo-42.example"` | Contact for payment reminders | yes |
| `phone` | string | `"+33199000042"` | Contact for payment reminders (a string: a number would drop the `+`) | yes |
| `street` | string | `"12 rue des Exemples"` | Street address | yes |
| `postal_code` | string | `"69002"` | A string: a number would turn `01000` into `1000` | |
| `city` | string | `"Lyon"` | City | |
| `country` | string (ISO 3166-1 alpha-2) | `"FR"` | Country | |
| `payment_terms_days` | integer | `30` | Payment terms granted to this customer, in days | |

The history of a customer (for example an address change) is not stored in the events: each event is
one state, and the sequence of events is the history. dbt builds it as an SCD2 dimension in M6.

## Invoice events

`invoice_issued` and `invoice_updated` have the same fields (full state).

| Field | Type | Example | Meaning |
|---|---|---|---|
| `invoice_id` | string | `"inv-000123"` | Invoice ID: links all the events of one invoice (not unique per event) |
| `customer_id` | string | `"cus-0042"` | Billed customer |
| `issue_date` | string (date) | `"2026-03-02"` | Legal date printed on the invoice: sets the revenue month and starts the payment delay |
| `due_date` | string (date) | `"2026-04-01"` | Payment deadline: an invoice unpaid after this date is late |
| `currency` | string (ISO 4217) | `"EUR"` | Currency of every amount below |
| `amount_excl_vat` | string (decimal) | `"1000.00"` | Amount excluding VAT: used for revenue |
| `vat_rate` | string (decimal) | `"0.20"` | One of `0.20`, `0.10`, `0.055`, `0.021` |
| `vat_amount` | string (decimal) | `"200.00"` | VAT amount |
| `amount_incl_vat` | string (decimal) | `"1200.00"` | Amount including VAT: what the customer must pay |

`invoice_cancelled`:

| Field | Type | Example | Meaning |
|---|---|---|---|
| `invoice_id` | string | `"inv-000123"` | Cancelled invoice |
| `cancellation_reason` | string: `duplicate`, `customer_request` or `billing_error` | `"billing_error"` | Why it was cancelled |

## Payment events

`payment_received`:

| Field | Type | Example | Meaning |
|---|---|---|---|
| `payment_id` | string | `"pay-000456"` | Payment ID |
| `invoice_id` | string | `"inv-000123"` | Paid invoice (also the Kafka key) |
| `payment_date` | string (date) | `"2026-03-28"` | Date the money was received: used for the payment delay (DSO) |
| `amount` | string (decimal) | `"600.00"` | Amount received. A payment can be partial: an invoice can have several payments |
| `currency` | string (ISO 4217) | `"EUR"` | Currency of the amount |

`payment_refunded`:

| Field | Type | Example | Meaning |
|---|---|---|---|
| `refund_id` | string | `"ref-000007"` | Refund ID |
| `payment_id` | string | `"pay-000456"` | Refunded payment |
| `invoice_id` | string | `"inv-000123"` | Invoice of that payment (also the Kafka key) |
| `refund_date` | string (date) | `"2026-04-03"` | Date the money was sent back |
| `amount` | string (decimal) | `"600.00"` | Refunded amount, at most the payment amount |
| `currency` | string (ISO 4217) | `"EUR"` | Currency of the amount |

## Rules a valid event follows

- `amount_incl_vat = amount_excl_vat + vat_amount`, and `vat_amount` is `amount_excl_vat × vat_rate`
  rounded to the cent (half up).
- `due_date = issue_date + payment_terms_days` of the customer at issue time.
- Every amount is greater than zero, and `currency` is `"EUR"`.
- A payment refers to an existing invoice, and the payments of an invoice never exceed its `amount_incl_vat`.
- Events can arrive late (up to 3 days): `event_time` stays the time of the action, not the arrival time.

The generator breaks some of these rules on purpose (the anomalies), and the platform must detect them.

## Version 2

Only `payment_received` changes: it gains one field, and its `schema_version` becomes `2`.

| Field | Type | Example | Meaning |
|---|---|---|---|
| `payment_method` | string: `bank_transfer`, `card` or `direct_debit` | `"bank_transfer"` | How the customer paid |

In version 1 the field is absent (not `null`). Consumers must read both versions without failing.
Every other event stays at version 1.

## Example

```json
{
  "event_id": "3f6c1e2a-9b4d-4c1e-8a7f-2d5e6b7c8a90",
  "event_type": "invoice_issued",
  "event_time": "2026-03-02T09:15:00Z",
  "schema_version": 1,
  "company_id": "cmp-0001",
  "invoice_id": "inv-000123",
  "customer_id": "cus-0042",
  "issue_date": "2026-03-02",
  "due_date": "2026-04-01",
  "currency": "EUR",
  "amount_excl_vat": "1000.00",
  "vat_rate": "0.20",
  "vat_amount": "200.00",
  "amount_incl_vat": "1200.00"
}
```

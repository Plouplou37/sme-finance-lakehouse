"""Build the events of docs/event-contract.md: one function per event type.

These functions only format data. Deciding what happens, and when, is the job of lifecycle.py.
"""

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from random import Random

from generator.config import CURRENCY

TOPIC_BY_EVENT_TYPE = {
    "customer_created": "finance.customers.v1",
    "customer_updated": "finance.customers.v1",
    "invoice_issued": "finance.invoices.v1",
    "invoice_updated": "finance.invoices.v1",
    "invoice_cancelled": "finance.invoices.v1",
    "payment_received": "finance.payments.v1",
    "payment_refunded": "finance.payments.v1",
}
TOPICS = sorted(set(TOPIC_BY_EVENT_TYPE.values()))
KEY_FIELD_BY_TOPIC = {
    "finance.customers.v1": "customer_id",
    "finance.invoices.v1": "invoice_id",
    "finance.payments.v1": "invoice_id",
}
CANCELLATION_REASONS = ("duplicate", "customer_request", "billing_error")
PAYMENT_METHODS = ("bank_transfer", "card", "direct_debit")  # schema version 2 only
CENT = Decimal("0.01")


@dataclass
class Customer:
    """A customer of a company. payer_profile drives the simulation and is never sent in an event."""

    customer_id: str
    company_id: str
    customer_type: str
    name: str
    email: str
    phone: str
    street: str
    postal_code: str
    city: str
    country: str
    payment_terms_days: int
    payer_profile: str


@dataclass
class Invoice:
    invoice_id: str
    company_id: str
    customer_id: str
    issue_date: date
    due_date: date
    amount_excl_vat: Decimal
    vat_rate: Decimal

    @property
    def vat_amount(self) -> Decimal:
        return (self.amount_excl_vat * self.vat_rate).quantize(CENT, rounding=ROUND_HALF_UP)  # contract rule

    @property
    def amount_incl_vat(self) -> Decimal:
        return self.amount_excl_vat + self.vat_amount


@dataclass
class Record:
    """One event as it leaves the application, plus what the simulation knows about it."""

    event: dict
    arrival_time: datetime  # when it reaches Kafka: equal to event_time, later for a late event
    anomaly: str | None = None  # set by anomalies.py, counted in the manifest

    @property
    def topic(self) -> str:
        return TOPIC_BY_EVENT_TYPE[self.event["event_type"]]

    @property
    def key(self) -> str:
        """Kafka key of the event (used by the producer in M3)."""
        return self.event[KEY_FIELD_BY_TOPIC[self.topic]]


def money(amount: Decimal) -> str:
    """Contract format for amounts: a decimal string with 2 decimals, such as "1200.00"."""
    return str(amount.quantize(CENT, rounding=ROUND_HALF_UP))


def timestamp(moment: datetime) -> str:
    """Contract format for event_time: ISO 8601 in UTC, such as "2026-03-02T09:15:00Z"."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_event_id(rng: Random) -> str:
    """A UUID drawn from rng. uuid.uuid4() would use the system's randomness and break --seed."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _record(rng: Random, event_type: str, moment: datetime, company_id: str, fields: dict, schema_version: int = 1) -> Record:
    # Values are copied into a new dict: changing the Customer or Invoice later does not change this event.
    event = {
        "event_id": new_event_id(rng),
        "event_type": event_type,
        "event_time": timestamp(moment),
        "schema_version": schema_version,
        "company_id": company_id,
        **fields,
    }
    return Record(event=event, arrival_time=moment)


# --- Customers: full state in both events ---


def _customer_fields(customer: Customer) -> dict:
    return {
        "customer_id": customer.customer_id,
        "customer_type": customer.customer_type,
        "name": customer.name,
        "email": customer.email,
        "phone": customer.phone,
        "street": customer.street,
        "postal_code": customer.postal_code,
        "city": customer.city,
        "country": customer.country,
        "payment_terms_days": customer.payment_terms_days,
    }


def customer_created(rng: Random, moment: datetime, customer: Customer) -> Record:
    return _record(rng, "customer_created", moment, customer.company_id, _customer_fields(customer))


def customer_updated(rng: Random, moment: datetime, customer: Customer) -> Record:
    return _record(rng, "customer_updated", moment, customer.company_id, _customer_fields(customer))


# --- Invoices: full state in issued and updated ---


def _invoice_fields(invoice: Invoice) -> dict:
    return {
        "invoice_id": invoice.invoice_id,
        "customer_id": invoice.customer_id,
        "issue_date": invoice.issue_date.isoformat(),
        "due_date": invoice.due_date.isoformat(),
        "currency": CURRENCY,
        "amount_excl_vat": money(invoice.amount_excl_vat),
        "vat_rate": str(invoice.vat_rate),
        "vat_amount": money(invoice.vat_amount),
        "amount_incl_vat": money(invoice.amount_incl_vat),
    }


def invoice_issued(rng: Random, moment: datetime, invoice: Invoice) -> Record:
    return _record(rng, "invoice_issued", moment, invoice.company_id, _invoice_fields(invoice))


def invoice_updated(rng: Random, moment: datetime, invoice: Invoice) -> Record:
    return _record(rng, "invoice_updated", moment, invoice.company_id, _invoice_fields(invoice))


def invoice_cancelled(rng: Random, moment: datetime, invoice: Invoice, reason: str) -> Record:
    if reason not in CANCELLATION_REASONS:
        raise ValueError(f"unknown cancellation reason: {reason}")
    fields = {"invoice_id": invoice.invoice_id, "cancellation_reason": reason}
    return _record(rng, "invoice_cancelled", moment, invoice.company_id, fields)


# --- Payments ---


def payment_received(
    rng: Random,
    moment: datetime,
    payment_id: str,
    invoice: Invoice,
    payment_date: date,
    amount: Decimal,
    payment_method: str | None = None,
) -> Record:
    """Version 1 without payment_method; version 2 when a payment_method is given."""
    fields = {
        "payment_id": payment_id,
        "invoice_id": invoice.invoice_id,
        "payment_date": payment_date.isoformat(),
        "amount": money(amount),
        "currency": CURRENCY,
    }
    if payment_method is None:
        return _record(rng, "payment_received", moment, invoice.company_id, fields)
    if payment_method not in PAYMENT_METHODS:
        raise ValueError(f"unknown payment method: {payment_method}")
    fields["payment_method"] = payment_method
    return _record(rng, "payment_received", moment, invoice.company_id, fields, schema_version=2)


def payment_refunded(
    rng: Random,
    moment: datetime,
    refund_id: str,
    payment_id: str,
    invoice: Invoice,
    refund_date: date,
    amount: Decimal,
) -> Record:
    fields = {
        "refund_id": refund_id,
        "payment_id": payment_id,
        "invoice_id": invoice.invoice_id,
        "refund_date": refund_date.isoformat(),
        "amount": money(amount),
        "currency": CURRENCY,
    }
    return _record(rng, "payment_refunded", moment, invoice.company_id, fields)

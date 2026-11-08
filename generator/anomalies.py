"""Inject the anomalies of the specification into the simulated events (M2, step 5).

An anomaly either changes one event or adds one event, and never spreads to other events:
a broken invoice is a new invoice with no payment, and a broken payment is the only one touched.
Silver should therefore reject exactly the events the manifest counts, type by type.

Order matters: content changes, then new events, then the schema change, then what happens in
transport (late arrival, duplicates), so that a duplicate is an exact copy of what was sent.
"""

import itertools
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from random import Random

from generator import events
from generator.config import Config
from generator.events import PAYMENT_METHODS, Record

UNKNOWN_CURRENCIES = ("EURO", "XYZ", "€")


class _FakeIds:
    """IDs outside the ranges of the simulation (inv-900001 vs inv-000001), so they never match a real one.

    Safe as long as a run has fewer than 800,000 invoices or payments.
    """

    def __init__(self) -> None:
        self._counters: dict[str, itertools.count] = {}

    def next(self, prefix: str) -> str:
        counter = self._counters.setdefault(prefix, itertools.count(1))
        return f"{prefix}{next(counter):05d}"


def inject(records: list[Record], cfg: Config, rng: Random) -> list[Record]:
    rates = cfg.anomaly_rates
    fake_ids = _FakeIds()
    _break_payments(records, rates, rng)
    records = records + _broken_invoices(records, rates, rng, fake_ids)
    records = records + _orphan_payments(records, rates, rng, fake_ids)
    records = records + _overpayments(records, rates, rng, fake_ids)
    _switch_payments_to_v2(records, cfg, rng)
    _delay_arrivals(records, cfg, rng)
    return records + _duplicates(records, rates, rng)


def _draw_kind(rng: Random, rates: dict[str, float], kinds: tuple[str, ...]) -> str | None:
    """At most one anomaly per event: a single draw, compared with the rates added up."""
    draw = rng.random()
    threshold = 0.0
    for kind in kinds:
        threshold += rates[kind]
        if draw < threshold:
            return kind
    return None


def _break_payments(records: list[Record], rates: dict[str, float], rng: Random) -> None:
    """Payments with an amount of zero or below, or an unknown currency (changed in place).

    Payments of refunded invoices are left alone: their refund would point to a rejected payment.
    """
    refunded = {r.event["invoice_id"] for r in records if r.event["event_type"] == "payment_refunded"}
    for record in records:
        event = record.event
        if event["event_type"] != "payment_received" or event["invoice_id"] in refunded:
            continue
        kind = _draw_kind(rng, rates, ("non_positive_amount", "unknown_currency"))
        if kind == "non_positive_amount":
            event["amount"] = rng.choice(["0.00", "-" + event["amount"]])
        elif kind == "unknown_currency":
            event["currency"] = rng.choice(UNKNOWN_CURRENCIES)
        record.anomaly = kind


def _broken_invoices(records: list[Record], rates: dict[str, float], rng: Random, fake_ids: _FakeIds) -> list[Record]:
    """New invoices with a zero or negative amount, an unknown currency, or an unknown customer.

    Each one copies a real invoice_issued under a new invoice_id, and never gets a payment.
    """
    new = []
    for record in records:
        if record.event["event_type"] != "invoice_issued":
            continue
        kind = _draw_kind(rng, rates, ("non_positive_amount", "unknown_currency", "unknown_reference"))
        if kind is None:
            continue
        event = dict(record.event)
        event["event_id"] = events.new_event_id(rng)
        event["invoice_id"] = fake_ids.next("inv-9")
        if kind == "non_positive_amount":
            negative = rng.random() < 0.5
            for name in ("amount_excl_vat", "vat_amount", "amount_incl_vat"):
                event[name] = "-" + event[name] if negative else "0.00"
        elif kind == "unknown_currency":
            event["currency"] = rng.choice(UNKNOWN_CURRENCIES)
        else:
            event["customer_id"] = fake_ids.next("cus-9")  # no customer_created with this ID
        new.append(Record(event=event, arrival_time=record.arrival_time, anomaly=kind))
    return new


def _orphan_payments(records: list[Record], rates: dict[str, float], rng: Random, fake_ids: _FakeIds) -> list[Record]:
    """New payments of an invoice that was never issued."""
    new = []
    for record in records:
        if record.event["event_type"] != "payment_received" or record.anomaly:
            continue
        if rng.random() < rates["unknown_reference"]:
            event = dict(record.event)
            event["event_id"] = events.new_event_id(rng)
            event["payment_id"] = fake_ids.next("pay-9")
            event["invoice_id"] = fake_ids.next("inv-8")  # never issued
            new.append(Record(event=event, arrival_time=record.arrival_time, anomaly="unknown_reference"))
    return new


def _overpayments(records: list[Record], rates: dict[str, float], rng: Random, fake_ids: _FakeIds) -> list[Record]:
    """A fully paid invoice receives the same payment again, a few days later: the customer paid twice."""
    amount_due: dict[str, Decimal] = {}  # invoice_id -> amount incl. VAT of its latest version
    received: dict[str, Decimal] = {}  # invoice_id -> total of its payments
    last_payment: dict[str, Record] = {}
    refunded: set[str] = set()
    for record in records:
        if record.anomaly:
            continue  # broken events are not part of the normal life of an invoice
        event = record.event
        invoice_id = event.get("invoice_id")
        if event["event_type"] in ("invoice_issued", "invoice_updated"):
            amount_due[invoice_id] = Decimal(event["amount_incl_vat"])
        elif event["event_type"] == "payment_received":
            received[invoice_id] = received.get(invoice_id, Decimal(0)) + Decimal(event["amount"])
            if invoice_id not in last_payment or record.arrival_time > last_payment[invoice_id].arrival_time:
                last_payment[invoice_id] = record
        elif event["event_type"] == "payment_refunded":
            refunded.add(invoice_id)

    new = []
    for invoice_id, payment in last_payment.items():
        if invoice_id in refunded or received[invoice_id] != amount_due.get(invoice_id):
            continue  # only invoices paid exactly, and never refunded
        if rng.random() < rates["overpayment"]:
            moment = payment.arrival_time + timedelta(days=rng.randint(1, 10))
            event = dict(payment.event)
            event["event_id"] = events.new_event_id(rng)
            event["payment_id"] = fake_ids.next("pay-9")
            event["event_time"] = events.timestamp(moment)
            event["payment_date"] = moment.date().isoformat()
            new.append(Record(event=event, arrival_time=moment, anomaly="overpayment"))
    return new


def _switch_payments_to_v2(records: list[Record], cfg: Config, rng: Random) -> None:
    """From cfg.schema_v2_start on, the application sends payment_received in version 2 (with payment_method)."""
    start = datetime.combine(cfg.schema_v2_start, time.min, tzinfo=timezone.utc)
    for record in records:
        event = record.event
        if event["event_type"] != "payment_received":
            continue
        event_time = datetime.strptime(event["event_time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if event_time >= start:
            event["schema_version"] = 2
            event["payment_method"] = rng.choice(PAYMENT_METHODS)


def _delay_arrivals(records: list[Record], cfg: Config, rng: Random) -> None:
    """Events that reach Kafka 1 hour to 3 days after they happened. Their event_time does not change.

    A delay that would push an event past the end of the period is not applied: the event would be
    dropped, while its payments, already arrived, would point to an invoice missing from the files.
    """
    end = datetime.combine(cfg.end_date, time.min, tzinfo=timezone.utc)
    for record in records:
        if record.anomaly is None and rng.random() < cfg.anomaly_rates["late_arrival"]:
            delayed = record.arrival_time + timedelta(seconds=rng.randint(3600, 3 * 86400))
            if delayed < end:
                record.arrival_time = delayed
                record.anomaly = "late_arrival"


def _duplicates(records: list[Record], rates: dict[str, float], rng: Random) -> list[Record]:
    """Exact copies, same event_id, sent again 1 second to 10 minutes later: a producer retry."""
    new = []
    for record in records:
        if rng.random() < rates["duplicate"]:
            later = record.arrival_time + timedelta(seconds=rng.randint(1, 600))
            new.append(Record(event=dict(record.event), arrival_time=later, anomaly="duplicate"))
    return new

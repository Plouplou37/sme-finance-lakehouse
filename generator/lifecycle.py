"""Simulate customers and invoices, and return the events their lives produce.

State machine and shares: see config.py. Every random draw goes through `rng`, never through the
`random` module itself, so that the same --seed always gives the same events.
"""

import itertools
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from random import Random

from generator import config, events
from generator.events import Customer, Invoice, Record

# ---------------------------------------------------------------------------
# Provided helpers
# ---------------------------------------------------------------------------


class Ids:
    """Sequential business IDs, unique across all companies: inv-000001, inv-000002..."""

    def __init__(self) -> None:
        self._counters: dict[str, itertools.count] = {}

    def next(self, prefix: str, width: int = 6) -> str:
        counter = self._counters.setdefault(prefix, itertools.count(1))
        return f"{prefix}-{next(counter):0{width}d}"


def pick(rng: Random, shares: dict):
    """Draw one key of `shares`, using its value as probability.

    pick(rng, {"a": 0.8, "b": 0.2}) returns "a" about 8 times out of 10.
    """
    return rng.choices(list(shares), weights=list(shares.values()))[0]


def draw_in_ranges(rng: Random, ranges: dict) -> int:
    """Draw a range with its share, then a whole number inside it (example: config.LATE_DAYS_AFTER_DUE)."""
    low, high = pick(rng, ranges)
    return rng.randint(low, high)


def draw_amount_excl_vat(rng: Random) -> Decimal:
    """An amount in euros, drawn as a whole number of cents: no float, so the amount is exact."""
    low, high = pick(rng, config.AMOUNT_RANGES)
    return Decimal(rng.randint(low * 100, high * 100)) / 100


def moment_on(rng: Random, day: date, first_hour: int = 7, last_hour: int = 17) -> datetime:
    """A random time on `day`, during business hours, in UTC."""
    clock = time(rng.randint(first_hour, last_hour), rng.randint(0, 59), rng.randint(0, 59))
    return datetime.combine(day, clock, tzinfo=timezone.utc)


# Obviously fictional names, and the .example domain, reserved so that it never exists (RFC 2606).
_FIRST_NAMES = ["Camille", "Dominique", "Alex", "Claude", "Sacha", "Morgan", "Charlie", "Lou"]
_BUSINESS_WORDS = ["Atelier", "Studio", "Cabinet", "Maison", "Garage", "Agence", "Boutique", "Ferme"]
_STREETS = ["rue des Exemples", "avenue de la Demo", "place du Test", "boulevard Fictif", "chemin des Essais"]
_CITIES = [
    ("69002", "Lyon"),
    ("59000", "Lille"),
    ("44000", "Nantes"),
    ("33000", "Bordeaux"),
    ("31000", "Toulouse"),
    ("13001", "Marseille"),
    ("67000", "Strasbourg"),
    ("01000", "Bourg-en-Bresse"),  # leading zero: the reason postal_code is a string
]


def fake_address(rng: Random) -> dict:
    postal_code, city = rng.choice(_CITIES)
    street = f"{rng.randint(1, 120)} {rng.choice(_STREETS)}"
    return {"street": street, "postal_code": postal_code, "city": city, "country": "FR"}


def fake_identity(rng: Random, customer_type: str, customer_id: str) -> dict:
    """Name, contact details and address of a customer, as keyword arguments for Customer(...)."""
    number = int(customer_id.split("-")[1])
    if customer_type == "company":
        name = f"{rng.choice(_BUSINESS_WORDS)} Demo {number}"
        email = f"contact@{name.lower().replace(' ', '-')}.example"
    else:
        first_name = rng.choice(_FIRST_NAMES)
        name = f"{first_name} Demo-{number}"
        email = f"{first_name.lower()}.demo{number}@mail.example"
    phone = f"+3319900{rng.randint(0, 9999):04d}"
    return {"name": name, "email": email, "phone": phone, **fake_address(rng)}


# ---------------------------------------------------------------------------
# To write (Quentin), one TODO at a time
# ---------------------------------------------------------------------------


def create_customers(rng: Random, ids: Ids, company_id: str) -> list[Customer]:
    """TODO 1: create the customers of one company.

    - How many: a whole number in config.CUSTOMERS_PER_COMPANY (rng.randint).
    - For each customer:
        customer_id          ids.next("cus", 4)
        customer_type        pick(rng, config.CUSTOMER_TYPES)
        payer_profile        drawn with the customer shares of config.PAYER_PROFILES
                             (each value is a pair: share of customers, share of late invoices)
        payment_terms_days   one of config.PAYMENT_TERMS_DAYS (rng.choice)
        name, email, phone   fake_identity(rng, customer_type, customer_id)
        and address          returns a dict: Customer(..., **identity) unpacks it into keyword arguments
    """
    return []


def simulate_invoice(rng: Random, ids: Ids, customer: Customer, issue_day: date) -> list[Record]:
    """TODO 2: simulate the whole life of one invoice and return its events, in order.

    Follow the state machine validated in M2:
    1. Create the Invoice (ids.next("inv"), amount, VAT rate, due date from the customer's terms)
       and its invoice_issued event.
    2. Sometimes (config.UPDATED_SHARE), push the due date back before any payment: invoice_updated.
    3. Decide the outcome: cancelled before any payment, never paid, or paid.
    4. If paid: on time or late (the customer's payer profile), in 1 payment or in 2 or 3.
    5. Sometimes (config.REFUNDED_SHARE), refund every payment, then cancel the invoice.
    Events can fall after the end of the period: main() drops them, as they have not happened yet.
    """
    return []


def simulate_address_change(rng: Random, customer: Customer, cfg: config.Config) -> list[Record]:
    """TODO 3: with config.ADDRESS_CHANGE_SHARE, move the customer once during the period.

    New address: fake_address(rng). Return one customer_updated event carrying the full, new state.
    """
    return []


# ---------------------------------------------------------------------------
# Provided: the whole simulation
# ---------------------------------------------------------------------------


def simulate(cfg: config.Config, rng: Random) -> list[Record]:
    """Run the simulation and return every event, in the order they were created."""
    ids = Ids()
    records: list[Record] = []
    for _ in range(cfg.companies):
        company_id = ids.next("cmp", 4)
        customers = create_customers(rng, ids, company_id)
        for customer in customers:
            # Customers exist before the first invoice: created on the first day, before business hours.
            moment = moment_on(rng, cfg.start_date, first_hour=6, last_hour=6)
            records.append(events.customer_created(rng, moment, customer))
        if not customers:
            continue  # nothing to invoice until TODO 1 is written

        for offset in range(cfg.days):
            day = cfg.start_date + timedelta(days=offset)
            for _ in range(rng.randint(*config.INVOICES_PER_COMPANY_PER_DAY)):
                customer = rng.choice(customers)
                records.extend(simulate_invoice(rng, ids, customer, day))

        for customer in customers:
            records.extend(simulate_address_change(rng, customer, cfg))
    return records

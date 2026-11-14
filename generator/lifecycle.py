"""Simulate customers and invoices, and return the events their lives produce.

State machine and shares: see config.py. Every random draw goes through `rng`, never through the
`random` module itself, so that the same --seed always gives the same events.
"""

import itertools
from datetime import UTC, date, datetime, time, timedelta
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


def split_amount(rng: Random, total: Decimal, parts: int) -> list[Decimal]:
    """Split `total` into `parts` amounts that add up exactly to it (computed in whole cents)."""
    remaining = int(total * 100)
    amounts = []
    for _ in range(parts - 1):
        cents = remaining * rng.randint(30, 60) // 100  # each earlier payment: 30 to 60 % of what is left
        amounts.append(Decimal(cents) / 100)
        remaining -= cents
    amounts.append(Decimal(remaining) / 100)  # the last payment settles the rest, to the cent
    return amounts


def moment_on(rng: Random, day: date, first_hour: int = 7, last_hour: int = 17) -> datetime:
    """A random time on `day`, during business hours, in UTC."""
    clock = time(rng.randint(first_hour, last_hour), rng.randint(0, 59), rng.randint(0, 59))
    return datetime.combine(day, clock, tzinfo=UTC)


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
# Lifecycle (TODO 1 to 3)
# ---------------------------------------------------------------------------


def create_customers(rng: Random, ids: Ids, company_id: str) -> list[Customer]:
    """Create the customers of one company (TODO 1, done).

    - How many: a whole number in config.CUSTOMERS_PER_COMPANY.
    - Each customer gets a type, a payer profile, payment terms and a fictional identity.
    """
    # pick() needs {profile: share of customers}. The second value of each pair (share of late
    # invoices) is used later, when an invoice is paid.
    profile_shares = {profile: share for profile, (share, _late_share) in config.PAYER_PROFILES.items()}

    customers = []
    for _ in range(rng.randint(*config.CUSTOMERS_PER_COMPANY)):
        customer_id = ids.next("cus", 4)
        customer_type = pick(rng, config.CUSTOMER_TYPES)
        identity = fake_identity(rng, customer_type, customer_id)  # needs the type and the ID drawn above
        customers.append(
            Customer(
                customer_id=customer_id,
                company_id=company_id,
                customer_type=customer_type,
                payment_terms_days=rng.choice(config.PAYMENT_TERMS_DAYS),
                payer_profile=pick(rng, profile_shares),
                **identity,  # name, email, phone, street, postal_code, city, country
            )
        )
    return customers


def simulate_invoice(rng: Random, ids: Ids, customer: Customer, issue_day: date) -> list[Record]:
    """Simulate the whole life of one invoice and return its events, in order (TODO 2).

    State machine validated in M2: issued, sometimes updated, then cancelled before any payment,
    never paid, or paid (on time or late, in 1 to 3 payments), and rarely refunded then cancelled.
    Events can fall after the end of the period: main() drops them, as they have not happened yet.
    """

    # 2a: the invoice and its invoice_issued event
    invoice = Invoice(
        invoice_id=ids.next("inv"),
        company_id=customer.company_id,
        customer_id=customer.customer_id,
        issue_date=issue_day,
        due_date=issue_day + timedelta(days=customer.payment_terms_days),
        amount_excl_vat=draw_amount_excl_vat(rng),
        vat_rate=pick(rng, config.VAT_RATES),
    )
    records = [events.invoice_issued(rng, moment_on(rng, issue_day), invoice)]
    last_day = issue_day  # day of the latest event so far: the next ones come after it

    # 2b: sometimes the due date is pushed back a few days after issue, before any payment.
    # The invoice_issued event above keeps the old due date: events.py copied the values.
    if rng.random() < config.UPDATED_SHARE:
        updated_day = issue_day + timedelta(days=rng.randint(1, 10))
        invoice.due_date += timedelta(days=rng.choice(config.DUE_DATE_EXTENSIONS_DAYS))
        records.append(events.invoice_updated(rng, moment_on(rng, updated_day), invoice))
        last_day = updated_day

    # 2c: the outcome. Cancelled and never paid end the life of the invoice here.
    outcome = pick(
        rng,
        {
            "cancelled": config.CANCELLED_SHARE,
            "unpaid": config.UNPAID_SHARE,
            "paid": 1 - config.CANCELLED_SHARE - config.UNPAID_SHARE,
        },
    )
    if outcome == "cancelled":
        cancel_day = last_day + timedelta(days=rng.randint(1, 15))
        reason = rng.choice(events.CANCELLATION_REASONS)
        records.append(events.invoice_cancelled(rng, moment_on(rng, cancel_day), invoice, reason))
        return records
    if outcome == "unpaid":
        return records  # no event: the invoice simply stays unpaid

    # Paid: the payer profile decides whether the invoice is settled after its due date.
    # settle_day is the day of the last payment, when the payments add up to the amount incl. VAT.
    late_share = config.PAYER_PROFILES[customer.payer_profile][1]
    if rng.random() < late_share:
        settle_day = invoice.due_date + timedelta(days=draw_in_ranges(rng, config.LATE_DAYS_AFTER_DUE))
    else:
        settle_day = invoice.due_date - timedelta(days=draw_in_ranges(rng, config.ON_TIME_DAYS_BEFORE_DUE))
    first_possible_day = last_day + timedelta(days=1)
    settle_day = max(settle_day, first_possible_day)  # never paid before the invoice exists

    # 2d: 1 payment, or 2 or 3 on different days; the last one always falls on settle_day.
    payment_count = rng.randint(2, 3) if rng.random() < config.SPLIT_PAYMENT_SHARE else 1
    spare_days = (settle_day - first_possible_day).days  # free days before settle_day
    payment_count = min(payment_count, spare_days + 1)  # not enough days: fewer payments
    earlier_offsets = sorted(rng.sample(range(spare_days), payment_count - 1))
    payment_days = [first_possible_day + timedelta(days=offset) for offset in earlier_offsets]
    payment_days.append(settle_day)
    amounts = split_amount(rng, invoice.amount_incl_vat, payment_count)

    payments = []  # (payment_id, amount), kept in case of a refund
    for day, amount in zip(payment_days, amounts, strict=True):
        payment_id = ids.next("pay")
        records.append(events.payment_received(rng, moment_on(rng, day), payment_id, invoice, day, amount))
        payments.append((payment_id, amount))

    # 2e: rarely, every payment is refunded, then the invoice is cancelled at the customer's request.
    if rng.random() < config.REFUNDED_SHARE:
        refund_day = settle_day + timedelta(days=rng.randint(5, 30))
        for payment_id, amount in payments:
            refund_id = ids.next("ref")
            moment = moment_on(rng, refund_day)
            records.append(events.payment_refunded(rng, moment, refund_id, payment_id, invoice, refund_day, amount))
        cancel_day = refund_day + timedelta(days=1)
        records.append(events.invoice_cancelled(rng, moment_on(rng, cancel_day), invoice, "customer_request"))

    return records


def simulate_address_change(rng: Random, customer: Customer, cfg: config.Config) -> list[Record]:
    """Sometimes (config.ADDRESS_CHANGE_SHARE), move the customer once during the period (TODO 3).

    Return one customer_updated event carrying the full, new state (feeds the SCD2 of M6).
    """
    if rng.random() >= config.ADDRESS_CHANGE_SHARE:
        return []
    move_day = cfg.start_date + timedelta(days=rng.randint(1, cfg.days))  # after the customer was created
    for field, value in fake_address(rng).items():
        setattr(customer, field, value)  # street, postal_code, city, country
    return [events.customer_updated(rng, moment_on(rng, move_day), customer)]


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
            continue  # rng.choice() below needs at least one customer

        for offset in range(cfg.days):
            day = cfg.start_date + timedelta(days=offset)
            for _ in range(rng.randint(*config.INVOICES_PER_COMPANY_PER_DAY)):
                customer = rng.choice(customers)
                records.extend(simulate_invoice(rng, ids, customer, day))

        for customer in customers:
            records.extend(simulate_address_change(rng, customer, cfg))
    return records

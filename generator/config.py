"""Simulation parameters. Every value is fictional, chosen to look plausible (validated in M2).

Shares are probabilities between 0 and 1. Ranges are (min, max), both included.
A dict of ranges gives each range a share: a range is drawn first, then a number inside it.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal


@dataclass(frozen=True)
class Config:
    """Parameters of one run, set from the command line. Frozen: they cannot change during the run."""

    seed: int
    days: int
    companies: int
    start_date: date

    @property
    def end_date(self) -> date:
        """First day after the simulated period: nothing that happens from this day on is written."""
        return self.start_date + timedelta(days=self.days)


# --- Volume ---
CUSTOMERS_PER_COMPANY = (20, 40)
INVOICES_PER_COMPANY_PER_DAY = (0, 6)  # about 3 a day on average

# --- Customers ---
CUSTOMER_TYPES = {"company": 0.80, "individual": 0.20}
PAYMENT_TERMS_DAYS = (30, 45, 60)
# Payer profile: (share of customers, share of their invoices paid late). Overall about 19 % paid late.
PAYER_PROFILES = {
    "good": (0.60, 0.05),
    "average": (0.30, 0.30),
    "bad": (0.10, 0.70),
}
ADDRESS_CHANGE_SHARE = 0.10  # customers who move once during the period (feeds the SCD2 of M6)

# --- Invoices ---
AMOUNT_RANGES = {(50, 500): 0.50, (500, 2_000): 0.30, (2_000, 20_000): 0.20}  # euros excl. VAT: mostly small
VAT_RATES = {Decimal("0.20"): 0.85, Decimal("0.10"): 0.10, Decimal("0.055"): 0.04, Decimal("0.021"): 0.01}
CURRENCY = "EUR"
UPDATED_SHARE = 0.05  # due date pushed back, before any payment
DUE_DATE_EXTENSIONS_DAYS = (15, 30)

# --- Outcome of an invoice (shares of all invoices) ---
CANCELLED_SHARE = 0.02  # cancelled before any payment
UNPAID_SHARE = 0.02  # never paid
# The other 96 % are paid. Shares of the paid invoices:
SPLIT_PAYMENT_SHARE = 0.10  # paid in 2 or 3 payments
REFUNDED_SHARE = 0.01  # every payment refunded, then the invoice is cancelled (reason customer_request)

# --- When the payment arrives ---
ON_TIME_DAYS_BEFORE_DUE = {(0, 7): 0.70, (8, 30): 0.30}  # mostly in the last days before the due date
LATE_DAYS_AFTER_DUE = {(1, 15): 0.75, (16, 60): 0.25}  # mostly less than 15 days late

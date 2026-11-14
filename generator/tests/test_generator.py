"""Tests of the generator (M2): reproducibility, manifest, options, and one test per anomaly.

Run from the repository root:
    uv run pytest
"""

import json
import random
from collections import Counter
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from generator import anomalies, lifecycle
from generator.__main__ import main
from generator.config import ANOMALY_RATES, Config
from generator.events import Record

# Higher than the defaults, so that every anomaly appears several times in a small run.
TEST_RATES = {name: 0.05 for name in ANOMALY_RATES}
AMOUNT_FIELD = {
    "invoice_issued": "amount_incl_vat",
    "invoice_updated": "amount_incl_vat",
    "payment_received": "amount",
    "payment_refunded": "amount",
}
VALID_CONTENT = {None, "late_arrival"}  # a late event is still a valid event: only its arrival changed


def parse(timestamp: str) -> datetime:
    return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def with_anomaly(records: list[Record], kind: str) -> list[Record]:
    found = [record for record in records if record.anomaly == kind]
    assert found, f"no {kind} was injected: the test would check nothing"  # no silent success
    return found


def valid(records: list[Record]) -> list[Record]:
    return [record for record in records if record.anomaly in VALID_CONTENT]


@pytest.fixture(scope="module")
def run() -> tuple[Config, list[Record]]:
    """One in-memory run, shared by the anomaly tests (simulated once for the whole file)."""
    cfg = Config(seed=1, days=60, companies=3, start_date=date(2026, 1, 1), anomaly_rates=dict(TEST_RATES))
    rng = random.Random(cfg.seed)
    return cfg, anomalies.inject(lifecycle.simulate(cfg, rng), cfg, rng)


@pytest.fixture(scope="module")
def records(run: tuple[Config, list[Record]]) -> list[Record]:
    return run[1]


def run_cli(out: Path, *options: str) -> dict:
    """Run the command line into `out` and return the manifest. Later options win over earlier ones."""
    main(["--days", "30", "--companies", "2", "--out", str(out), *options])
    return json.loads((out / "manifest.json").read_text(encoding="utf-8"))


def file_contents(folder: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in sorted(folder.iterdir())}


# --- Reproducibility, manifest, options ---


def test_same_seed_gives_identical_files(tmp_path: Path) -> None:
    run_cli(tmp_path / "a", "--seed", "7")
    run_cli(tmp_path / "b", "--seed", "7")
    assert file_contents(tmp_path / "a") == file_contents(tmp_path / "b")


def test_other_seed_gives_other_events(tmp_path: Path) -> None:
    run_cli(tmp_path / "a", "--seed", "7")
    run_cli(tmp_path / "b", "--seed", "8")
    assert file_contents(tmp_path / "a") != file_contents(tmp_path / "b")


def test_manifest_matches_written_files(tmp_path: Path) -> None:
    manifest = run_cli(tmp_path, "--seed", "3")
    written = []
    for topic, count in manifest["events"]["by_topic"].items():
        lines = (tmp_path / f"{topic}.jsonl").read_text(encoding="utf-8").splitlines()
        assert len(lines) == count, topic
        written += [json.loads(line) for line in lines]
    assert len(written) == manifest["events"]["total"]
    assert Counter(event["event_type"] for event in written) == manifest["events"]["by_event_type"]
    assert Counter(str(event["schema_version"]) for event in written) == manifest["events"]["by_schema_version"]


def test_days_and_companies_are_configurable(tmp_path: Path) -> None:
    small = run_cli(tmp_path / "small", "--days", "10", "--companies", "1")
    large = run_cli(tmp_path / "large", "--days", "40", "--companies", "3")
    assert small["parameters"]["days"] == 10 and small["parameters"]["companies"] == 1
    assert small["events"]["by_event_type"]["invoice_issued"] < large["events"]["by_event_type"]["invoice_issued"]


def test_anomaly_rates_are_configurable(tmp_path: Path) -> None:
    zero = [arg for name in ANOMALY_RATES for arg in (f"--{name.replace('_', '-')}-rate", "0")]
    assert run_cli(tmp_path / "none", *zero)["anomalies"]["total"] == 0
    many = run_cli(tmp_path / "many", "--duplicate-rate", "0.5")["anomalies"]["by_type"]["duplicate"]
    default = run_cli(tmp_path / "default")["anomalies"]["by_type"]["duplicate"]
    assert many > default


# --- Valid events follow the contract ---


def test_valid_invoices_follow_the_contract_rules(records: list[Record]) -> None:
    terms = {r.event["customer_id"]: r.event["payment_terms_days"] for r in records if r.event["event_type"] == "customer_created"}
    for record in valid(records):
        event = record.event
        if event["event_type"] != "invoice_issued":
            continue
        excl, rate, vat = Decimal(event["amount_excl_vat"]), Decimal(event["vat_rate"]), Decimal(event["vat_amount"])
        assert Decimal(event["amount_incl_vat"]) == excl + vat
        assert vat == (excl * rate).quantize(Decimal("0.01"), rounding="ROUND_HALF_UP")
        due_in = date.fromisoformat(event["due_date"]) - date.fromisoformat(event["issue_date"])
        assert due_in.days == terms[event["customer_id"]]


# --- One test per anomaly: every labelled event shows it, and no valid event does ---


def test_duplicate_is_an_exact_copy_sent_later(records: list[Record]) -> None:
    originals = {r.event["event_id"]: r for r in records if r.anomaly != "duplicate"}
    assert len(originals) == len([r for r in records if r.anomaly != "duplicate"])  # event_id unique otherwise
    for duplicate in with_anomaly(records, "duplicate"):
        original = originals[duplicate.event["event_id"]]
        assert duplicate.event == original.event
        assert duplicate.arrival_time > original.arrival_time


def test_late_arrival_keeps_its_event_time(records: list[Record]) -> None:
    for record in with_anomaly(records, "late_arrival"):
        delay = record.arrival_time - parse(record.event["event_time"])
        assert timedelta(hours=1) <= delay <= timedelta(days=3)
    for record in records:
        if record.anomaly is None:
            assert record.arrival_time == parse(record.event["event_time"])


def test_non_positive_amount(records: list[Record]) -> None:
    for record in with_anomaly(records, "non_positive_amount"):
        assert Decimal(record.event[AMOUNT_FIELD[record.event["event_type"]]]) <= 0
    for record in valid(records):
        field = AMOUNT_FIELD.get(record.event["event_type"])
        if field:
            assert Decimal(record.event[field]) > 0


def test_unknown_currency(records: list[Record]) -> None:
    for record in with_anomaly(records, "unknown_currency"):
        assert record.event["currency"] != "EUR"
    for record in valid(records):
        if "currency" in record.event:
            assert record.event["currency"] == "EUR"


def test_unknown_reference(records: list[Record]) -> None:
    customers = {r.event["customer_id"] for r in records if r.event["event_type"] == "customer_created"}
    invoices = {r.event["invoice_id"] for r in records if r.event["event_type"] == "invoice_issued"}
    for record in with_anomaly(records, "unknown_reference"):
        if record.event["event_type"] == "invoice_issued":
            assert record.event["customer_id"] not in customers
        else:
            assert record.event["invoice_id"] not in invoices
    for record in valid(records):
        event = record.event
        if event["event_type"] in ("invoice_issued", "invoice_updated"):
            assert event["customer_id"] in customers
        if event["event_type"] in ("invoice_updated", "invoice_cancelled", "payment_received", "payment_refunded"):
            assert event["invoice_id"] in invoices


def test_overpayment(records: list[Record]) -> None:
    amount_due: dict[str, Decimal] = {}
    paid: Counter[str] = Counter()
    for record in valid(records):
        event = record.event
        if event["event_type"] in ("invoice_issued", "invoice_updated"):
            amount_due[event["invoice_id"]] = Decimal(event["amount_incl_vat"])
        elif event["event_type"] == "payment_received":
            paid[event["invoice_id"]] += Decimal(event["amount"])
    for invoice_id, total in paid.items():
        assert total <= amount_due[invoice_id], invoice_id  # valid payments never exceed the amount due
    for record in with_anomaly(records, "overpayment"):
        invoice_id = record.event["invoice_id"]
        assert paid[invoice_id] == amount_due[invoice_id]  # already fully paid...
        assert Decimal(record.event["amount"]) > 0  # ...so any extra payment is too much


def test_payments_switch_to_schema_v2_in_the_middle(run: tuple[Config, list[Record]]) -> None:
    cfg, records = run
    start = datetime.combine(cfg.schema_v2_start, time.min, tzinfo=UTC)
    payments = [r for r in records if r.event["event_type"] == "payment_received"]
    assert 0 < sum(p.event["schema_version"] == 2 for p in payments) < len(payments)  # both versions exist
    for payment in payments:
        is_v2 = parse(payment.event["event_time"]) >= start
        assert payment.event["schema_version"] == (2 if is_v2 else 1)
        assert ("payment_method" in payment.event) == is_v2
    for record in records:
        if record.event["event_type"] != "payment_received":
            assert record.event["schema_version"] == 1

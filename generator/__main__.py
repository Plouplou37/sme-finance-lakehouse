"""Generate fictional accounting events (format: docs/event-contract.md).

Run from the repository root:
    uv run python -m generator --seed 42 --days 90 --companies 5

Writes one JSON Lines file per topic and manifest.json into data/generated/ (ignored by Git).
"""

import argparse
import random
from datetime import date, datetime, time, timezone
from pathlib import Path

from generator import anomalies, lifecycle, writer
from generator.config import ANOMALY_RATES, Config


def positive_int(text: str) -> int:
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be 1 or more")
    return value


def share(text: str) -> float:
    value = float(text)
    if not 0 <= value <= 1:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m generator", description="Generate fictional accounting events.")
    parser.add_argument("--seed", type=int, default=42, help="same seed, same events (default: 42)")
    parser.add_argument("--days", type=positive_int, default=90, help="number of simulated days (default: 90)")
    parser.add_argument("--companies", type=positive_int, default=5, help="number of companies (default: 5)")
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date(2026, 1, 1),
        help="first simulated day, YYYY-MM-DD (default: 2026-01-01)",
    )
    parser.add_argument("--out", type=Path, default=Path("data/generated"), help="output folder (default: data/generated)")
    for name, default in ANOMALY_RATES.items():  # --duplicate-rate 0.05, --overpayment-rate 0...
        parser.add_argument(
            f"--{name.replace('_', '-')}-rate",
            dest=f"{name}_rate",
            type=share,
            default=default,
            help=f"share of events with this anomaly (default: {default})",
        )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rates = {name: getattr(args, f"{name}_rate") for name in ANOMALY_RATES}
    cfg = Config(seed=args.seed, days=args.days, companies=args.companies, start_date=args.start_date, anomaly_rates=rates)
    rng = random.Random(cfg.seed)  # the only source of randomness of the whole run

    records = lifecycle.simulate(cfg, rng)
    records = anomalies.inject(records, cfg, rng)

    # Keep only what has reached Kafka by the end of the period: later payments have not happened yet.
    end = datetime.combine(cfg.end_date, time.min, tzinfo=timezone.utc)
    received = [record for record in records if record.arrival_time < end]

    manifest = writer.write(received, cfg, args.out)
    print(f"{manifest['events']['total']} events written to {args.out}")
    for event_type, count in manifest["events"]["by_event_type"].items():
        print(f"  {event_type}: {count}")
    print(f"{manifest['anomalies']['total']} anomalies")
    for anomaly, count in manifest["anomalies"]["by_type"].items():
        print(f"  {anomaly}: {count}")


if __name__ == "__main__":
    main()

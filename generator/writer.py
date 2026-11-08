"""Write the events as JSON Lines (one file per topic) and the manifest."""

import json
from collections import Counter
from pathlib import Path

from generator.config import Config
from generator.events import TOPICS, Record


def write(records: list[Record], cfg: Config, out_dir: Path) -> dict:
    """Write the files in arrival order, the order the producer will publish them in (M3). Return the manifest."""
    out_dir.mkdir(parents=True, exist_ok=True)
    # sorted() is stable: events arriving at the same second keep the order the simulation created them in.
    ordered = sorted(records, key=lambda record: record.arrival_time)

    for topic in TOPICS:
        # newline="\n": on Windows, text mode would otherwise write "\r\n" line endings.
        with (out_dir / f"{topic}.jsonl").open("w", encoding="utf-8", newline="\n") as file:
            for record in ordered:
                if record.topic == topic:
                    file.write(json.dumps(record.event, ensure_ascii=False) + "\n")

    manifest = build_manifest(ordered, cfg)
    with (out_dir / "manifest.json").open("w", encoding="utf-8", newline="\n") as file:
        json.dump(manifest, file, indent=2)
        file.write("\n")
    return manifest


def build_manifest(records: list[Record], cfg: Config) -> dict:
    """Count what was written, from the written records themselves: the reference for later reconciliation.

    No generation timestamp on purpose: two runs with the same seed must give identical files.
    """
    by_topic = Counter(record.topic for record in records)
    by_event_type = Counter(record.event["event_type"] for record in records)
    by_anomaly = Counter(record.anomaly for record in records if record.anomaly)
    return {
        "parameters": {
            "seed": cfg.seed,
            "days": cfg.days,
            "companies": cfg.companies,
            "start_date": cfg.start_date.isoformat(),
            "end_date": cfg.end_date.isoformat(),
        },
        "events": {
            "total": len(records),
            "by_topic": {topic: by_topic[topic] for topic in TOPICS},
            "by_event_type": dict(sorted(by_event_type.items())),
        },
        "anomalies": {
            "total": sum(by_anomaly.values()),
            "by_type": dict(sorted(by_anomaly.items())),
        },
    }

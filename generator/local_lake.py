"""Prepare a local folder standing for s3://sme-lake, without S3, Kafka or Spark (M10: continuous integration).

1. Bronze: the generator's events, with the same columns and folders as the bronze that Spark writes from
   Kafka (M4): one row per line of the generator's files, the line unchanged in raw_json, with Kafka-like
   coordinates (the key of the event contract, a single partition, offsets in arrival order), in Hive-style
   folders <lake>/bronze/topic=<topic>/ingestion_date=<date>/.
2. One empty folder per silver model (<lake>/silver/<model>/). dbt-duckdb writes each one with DuckDB's
   COPY, which creates no parent folder: S3 has no folders to create, a local disk does. Gold is not written
   by dbt any more: it stays in the warehouse, and scripts/publish_gold.py publishes it as Iceberg (M12).

dbt then runs on it exactly as on the real lake:

    uv run python -m generator
    uv run python -m generator.local_lake --lake-root data/ci-lake
    LAKE_ROOT=data/ci-lake uv run dbt build --project-dir dbt --profiles-dir dbt --target ci

Rewrites the whole folder at each run (never appends), so running it twice changes nothing.
"""

import argparse
import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from generator.events import KEY_FIELD_BY_TOPIC

PUBLISHED_LAYERS = ("silver",)  # the dbt folders whose models dbt writes as files in the lake


def write_bronze(generated: Path, bronze: Path) -> int:
    ingested_at = datetime.now(UTC)
    rows = []
    for topic, key_field in KEY_FIELD_BY_TOPIC.items():
        lines = (generated / f"{topic}.jsonl").read_text(encoding="utf-8").splitlines()
        for offset, line in enumerate(lines):
            event = json.loads(line)
            rows.append(
                (
                    line,                                          # raw_json: the message, unchanged
                    event.get(key_field),                          # kafka_key: as the producer sets it (M3)
                    topic,
                    0,                                             # kafka_partition
                    offset,                                        # kafka_offset: the file is in arrival order
                    ingested_at + timedelta(milliseconds=offset),  # kafka_timestamp: keeps that order
                    ingested_at,
                    ingested_at.date(),                            # ingestion_date
                )
            )

    connection = duckdb.connect()
    connection.execute(
        """
        create table bronze (
            raw_json varchar, kafka_key varchar, topic varchar, kafka_partition integer, kafka_offset bigint,
            kafka_timestamp timestamptz, ingested_at timestamptz, ingestion_date date
        )
        """
    )
    connection.executemany("insert into bronze values (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    # Like Spark's partitionBy: topic and ingestion_date become folder names, not columns in the files.
    connection.execute(f"copy bronze to '{bronze.as_posix()}' (format parquet, partition_by (topic, ingestion_date))")
    for topic, count in connection.execute("select topic, count(*) from bronze group by topic order by topic").fetchall():
        print(f"bronze {topic}: {count} rows")
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--generated", default="data/generated", help="folder written by the generator")
    parser.add_argument("--lake-root", required=True, help="local folder standing for s3://sme-lake")
    parser.add_argument("--models", default="dbt/models", help="dbt's models folder")
    args = parser.parse_args()

    lake = Path(args.lake_root)
    shutil.rmtree(lake, ignore_errors=True)  # replace, never add files next to old ones (M0)
    lake.mkdir(parents=True)

    rows = write_bronze(Path(args.generated), lake / "bronze")

    folders = 0
    for layer in PUBLISHED_LAYERS:
        for model in sorted((Path(args.models) / layer).glob("*.sql")):
            (lake / layer / model.stem).mkdir(parents=True)
            folders += 1

    print(f"OK: {rows} rows of bronze and {folders} empty table folders in {lake.as_posix()}/")


if __name__ == "__main__":
    main()

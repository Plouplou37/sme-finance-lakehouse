# ADR 0006: Store the lake as Parquet files on S3, in bronze, silver and gold layers

- **Status:** Accepted (M1 for the bucket, M4 to M10 for the layout)
- **Date:** 2026-11-08, written down on 2026-11-14

## Context

Three engines read and write the same data: Spark (ingestion), DuckDB through dbt (transformations) and Athena through the Glue catalog (analysts). Storage must stay separate from compute, so that each can grow on its own, and must be cheap to keep: raw history is never deleted.

Users need different things: an audit trail of exactly what the application sent, clean and typed data for engineers, and simple tables answering business questions for analysts.

## Decision

1. **One bucket, `sme-lake`,** on Floci's S3 (ADR 0001), with one prefix per role: `bronze/`, `silver/`, `gold/`, `checkpoints/` (Spark), `athena-results/` (query outputs, kept out of every table folder).
2. **Parquet for every table:** columnar, typed and compressed, with statistics per column, so engines read only the columns and row groups they need. Every engine here reads it natively.
3. **Three layers (medallion):**

   | Layer | Content | Written by | Rewritten |
   |---|---|---|---|
   | Bronze | every Kafka message unchanged, partitioned by `topic=` and `ingestion_date=` | Spark (ADR 0005) | never, only appended |
   | Silver | valid events, typed and deduplicated, one table per entity; the quarantine (`silver_quarantine`) with the reason of each rejection and the raw JSON | dbt | in full at each run |
   | Gold | star schema (invoice and payment facts, customer history as SCD2, company and date dimensions) and one mart per analyst question | dbt | in full at each run |

4. **One folder per table** (`<layer>/<model>/<model>.parquet`): a Hive-style table is *everything under its prefix*, so a stray file in a table folder becomes rows of that table (M0, M9).
5. **Paths come from one setting:** dbt builds every location from `LAKE_ROOT` (`s3://sme-lake` by default) with the `lake_path` macro, so the same models run on S3 and on a local folder in the CI (M10).
6. **Every rewrite replaces, never appends** (silver and gold): a run can be replayed without creating duplicates.

## What was proven

| Check | Result |
|---|---|
| Three engines, the same files | ✅ Spark writes bronze, DuckDB reads it and writes silver and gold, Athena (floci-duck) reads gold through Glue: same results as DuckDB (M9) |
| Columnar reads | ✅ Spark's plan reads 2 of the bronze columns and never touches `raw_json` (M4) |
| A Hive-style table is its whole prefix | ✅ copying one more file into a table's folder doubled the query result, with no change in Glue (M0) |
| Silver and gold are rebuilt from bronze | ✅ every dbt build and daily run rebuilds them from bronze; replays give the same counts (M8) |

## Limits

This is a **lakehouse architecture without an open table format**: open files, a catalog and SQL engines over them, but plain Parquet folders instead of Iceberg or Delta tables.

- **No transactions:** a reader can see a table half rewritten; two writers can overwrite each other (one run at a time in Airflow avoids it).
- **No row updates or deletes:** erasing one customer's personal data means rewriting the files that hold it.
- **No history:** a rewritten table cannot be read as it was yesterday.
- **Small files** in bronze (ADR 0005); no compaction yet.
- **No partitions in silver and gold:** not needed at this size; every query reads the whole table.
- **Not tested on real S3:** durability, encryption, access policies and lifecycle rules depend on the service, not on Floci (ADR 0001). The `quarantine/` and `bench/` prefixes created in M1 are unused: the quarantine is a silver table, and `bench/` is kept for an optional benchmark.

## When not to use it

- **Small data, one engine, frequent updates:** a relational database (PostgreSQL) or a data warehouse is simpler, transactional and faster for lookups.
- **Tables updated row by row, or read "as of" a date:** an open table format (Apache Iceberg, Delta Lake) on the same S3 files adds transactions, updates and time travel.

## Alternatives considered

| Alternative | Why not |
|---|---|
| CSV or JSON files | No types, row-based: every query reads every column |
| A data warehouse only (Snowflake, BigQuery, Redshift) | Storage and compute coupled in one paid service; not available locally |
| Apache Iceberg | Adds transactions, updates, history and safe concurrent writes; more moving parts. Kept as an optional bonus |
| Partitioning gold by month or company | Useful at scale to read less; at this size it would only create small files |

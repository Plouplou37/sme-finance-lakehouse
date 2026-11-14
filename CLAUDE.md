# CLAUDE.md

Working rules for AI coding agents in this repository. A local data platform for a fictional
small-business accounting scenario: Kafka (Redpanda) → Spark → S3 bronze → dbt + DuckDB → silver
and gold → Glue + Athena, orchestrated by Airflow, with AWS emulated by Floci. Decisions and their
limits are in `docs/adr/`, progress in `docs/journal.md`.

## Ground rules

- No real cloud account, real credentials or paid service. AWS is emulated by Floci
  (`http://localhost:4566`); the credentials in `.env.example` are fake and stay fake.
- Synthetic data only: every event comes from `generator/`, with a seed.
- Never state that an emulated service behaves like AWS without a test that shows it. Record
  every gap found in the matching ADR.
- Do not install a tool on the host, or add a dependency, without the maintainer's approval.
- Propose commands that change state (containers, dependencies, Git history, GitHub settings) and
  let the maintainer run them. Read-only checks are fine.
- Nothing is pushed or merged without the maintainer. `main` only accepts pull requests with a
  green CI.

## Versions

- Never `latest`. Images are pinned by tag and digest, GitHub Actions by commit SHA, Python
  packages exactly in `uv.lock`; downloaded jars are checked against their Maven Central SHA-1.
- Choose releases at least 14 days old, checked on the official release page, not from memory.
- Every version change goes into `VERSIONS.md` with its reason.

## Commands

Local stack (Docker Compose, `.env` copied from `.env.example`):

```sh
make up         # Floci, floci-duck, Redpanda; bucket, prefixes and Kafka topics (idempotent)
make pipeline   # make up, then Airflow (http://localhost:8088) and Spark Connect
make down       # stop everything, keep the data
make reset      # stop everything and delete all data
```

Checks, the same as the CI, with no Docker and no credentials:

```sh
uv sync --locked
uv run ruff check .
uv run sqlfluff lint analytics
uv run pytest                       # one test: uv run pytest -k <name>
uv run python -m generator
uv run python -m generator.local_lake --lake-root data/ci-lake
LAKE_ROOT=data/ci-lake uv run dbt build --project-dir dbt --profiles-dir dbt --target ci
```

Against the local stack: `uv run --env-file .env dbt build --project-dir dbt --profiles-dir dbt --select <model>+`.

## Definition of done

- The checks above pass.
- A success criterion counts only with evidence (a count, a log line, a test result). Write what
  was tested and what was not.
- Every guard is triggered once on purpose: a broken contract, a blocking test, a failure.
- `docs/journal.md`, `README.md` and `VERSIONS.md` are updated; a decision with alternatives gets an ADR.

## Data conventions

- Bronze keeps each Kafka message unchanged (`raw_json`), never parsed. Spark writes it, nothing rewrites it.
- dbt layers: `staging`, `intermediate`, `gold` and `quality` stay in the DuckDB file; `silver` is
  published as Parquet with `config(location=lake_path('silver', '<model>'))`. Never hard-code an
  `s3://` path in a model.
- Gold reaches S3 only through `scripts/publish_gold.py`, as Iceberg tables in Glue, after every dbt
  test has passed. Schema changes come from the dbt contract; never change an Iceberg table by hand.
- One folder per table in the lake, and nothing else under a table's prefix.
- Deduplicate on `event_id` first, then validate. One rejection reason per event; rejected events
  go to the quarantine, never deleted.
- Amounts are `DECIMAL(18,2)`, never floating point. Timestamps are UTC.
- Gold: each description starts with the grain, contracts are enforced, every column is described,
  personal-data columns carry `meta.contains_pii`.
- A test blocks (`error`) when publishing would give wrong numbers, and warns when a signal drifts.
  Never weaken a test or a contract to make a build pass.
- Engine-specific SQL goes behind `adapter.dispatch` macros. Analysis queries in `analytics/` use
  SQL common to Trino and DuckDB (`date_diff`, `interval '90' day`, no `group by all`).
- Every task and script is idempotent: running it twice gives the same result.

## Gotchas

- Floci's Athena runs DuckDB, not Trino: a query in DuckDB-only syntax can return `SUCCEEDED`
  with no result. Treat that as an error.
- A dbt test that reads several models does not skip every model below it. The publication
  barrier is the DAG: `dbt_audit`, then `dbt_publish`, then `publish_gold`.
- Never read an Iceberg table's folder with `read_parquet`: it also holds the files of old snapshots.
  Follow Glue's pointer (`iceberg_scan` on `metadata_location`).
- PyIceberg's `expire_snapshots` deletes no file: unreferenced files are removed by `publish_gold.py`,
  only when older than a day.
- On a local disk, DuckDB's `COPY` creates no folders (S3 needs none): `generator/local_lake.py`
  creates them for the CI.
- A Spark checkpoint and its output folder are a pair: reset both or neither.
- Running the producer twice publishes every event twice. Expected: silver removes them on `event_id`.
- In Airflow 3, a run triggered from the command line has no logical date (`{{ ds }}` is undefined).
- The development host is Windows: in PowerShell, set variables with `$env:LAKE_ROOT = "data/ci-lake"`.
  Makefile recipes are single commands, so they run in `cmd.exe` and in `sh`.

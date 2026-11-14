# ADR 0007: Transform bronze into silver and gold with dbt on DuckDB

- **Status:** Accepted (M0 for the adapter, M5 to M7 for the project)
- **Date:** 2026-11-11, written down on 2026-11-14

## Context

Bronze holds raw JSON. Turning it into clean events (silver) and into tables for analysts (gold) is mostly SQL: parsing, typing, deduplicating, validating, joining, aggregating. These transformations must be tested, documented and traceable from a gold column back to bronze, and they must run for free, on a laptop and in the CI.

Athena cannot build the tables on Floci: dbt-athena creates them with `CREATE TABLE AS SELECT`, which Floci passes to DuckDB unchanged (ADR 0001).

## Decision

1. **dbt-core 1.12.6 with dbt-duckdb 1.11.1 and DuckDB 1.5.6.** The warehouse is a single DuckDB file; DuckDB reads bronze on S3 through its httpfs extension, and silver is published as Parquet on S3 (dbt-duckdb's `external` materialization). Gold was published the same way until M11; since M12 it stays as tables in the DuckDB file, and `scripts/publish_gold.py` publishes it as Iceberg tables in Glue once every test has passed (ADR 0010).
2. **Layers of models:**
   - `staging`: parse the envelope once; keep one row per `event_id` (incremental: later runs read only the rows ingested since the last run, minus a 3-day margin).
   - `intermediate`: one model per event type, typed fields (amounts as `DECIMAL(18,2)`), and a single rejection reason per event.
   - `silver`: valid events per entity, and the quarantine.
   - `gold`: facts, dimensions and marts (ADR 0006).
   - `quality`: the reconciliation with the generator's manifest.
3. **Deduplicate first, then validate:** a copy of an invalid event counts as a duplicate, not as a second rejection.
4. **Engine-specific SQL behind macros** (`adapter.dispatch`): JSON extraction and safe casts. Moving to another engine means rewriting two macros, not every model.
5. **Quality as code:**
   - generic and custom tests: uniqueness, not null, accepted values, relationships between tables, SCD2 validity, reconciliations from bronze to silver and from silver to gold, to the cent;
   - **enforced contracts** on the 9 gold tables (column names and types checked before writing);
   - severities: blocking (`error`) when publishing would give wrong numbers, warning when a signal drifts (more than 1 % of events quarantined warns, more than 5 % blocks);
   - source freshness on bronze, every gold column described, personal-data columns marked (`meta.contains_pii`);
   - `dbt docs` for the catalogue and the lineage graph.
6. **Write-Audit-Publish across two dbt runs, ordered by Airflow** (ADR 0008): `dbt_audit` builds and tests staging and intermediate (inside the DuckDB file, never read by users); `dbt_publish` writes silver and builds gold and quality only if the audit passed; since M12, `publish_gold` publishes gold only if every test of `dbt_publish` passed.
7. **One configurable lake root** (`LAKE_ROOT`, `lake_path` macro) and a `ci` target, so the same project runs on S3 and on a local folder (ADR 0009).

## What was proven

| Check | Result |
|---|---|
| Full build | ✅ 19 models and 54 tests: 72 pass, 1 warning (1.19 % quarantined), in about 4 seconds |
| Every bronze event accounted for | ✅ 2,578 = silver + 30 quarantined + 59 duplicates |
| Anomalies found match the manifest, type by type | ✅ duplicates 59, non-positive amounts 7, unknown currencies 11, unknown references 10, overpayments 2: a blocking test |
| Gold matches silver | ✅ number of invoices, totals including and excluding VAT, net amount received, to the cent |
| A contract break is caught | ✅ `first_seen_at` cast to a date: dbt stopped with the mismatch table, the published file kept its previous version; the same break was blocked by the CI (M10) |
| The barrier stops publication | ✅ quarantine threshold lowered to 0.5 %: `dbt_audit` failed, `dbt_publish` never ran, every silver and gold file kept its previous time (M8) |

## Limits

- **One machine, one writer:** DuckDB allows a single writing process, so runs are serialized (one active run in Airflow).
- **`external` models cannot be incremental:** silver is rewritten in full at each run, and so is gold before its publication. Fine at this size; at scale, the partitions touched by a run would be replaced (Iceberg makes it possible, ADR 0010).
- **Write-Audit-Publish is partial for silver:** tests on silver itself run after its files are written. Gold no longer has this limit since M12: it is written and tested in DuckDB, then published at once.
- **One environment:** the same target is used by hand and by Airflow; no separate production target.
- **Not done yet:** dbt unit tests on mocked rows, lint of the models' SQL, building only what a pull request changed (Slim CI).
- **A multi-model test does not stop every model below it** in a single `dbt build` (only those whose ancestors include all the models it reads): found in M8, and the reason the barrier lives in the DAG.

## When not to use it

- **Data larger than one machine,** or many people writing and querying at once: dbt stays, but on a warehouse (Snowflake, BigQuery, Redshift) or a distributed engine (Spark, Trino, Athena).
- **Logic that is not SQL-shaped:** machine-learning features, calls to external services, event-by-event processing in seconds. dbt runs batch SQL.

## Alternatives considered

| Alternative | Why not |
|---|---|
| dbt-athena | Builds tables with CTAS, which Floci does not emulate (ADR 0001) |
| Spark SQL for the transformations | Heavier for this volume; an optional bonus compares both on the same models |
| Plain SQL scripts or pandas | No dependency graph, tests, contracts or documentation for free |
| dbt snapshots for customer history | Snapshots capture a table that overwrites its rows; the events already record each change with its time, so the SCD2 is computed from them |

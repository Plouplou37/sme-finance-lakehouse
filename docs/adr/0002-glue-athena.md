# ADR 0002: Expose the gold layer through the Glue catalog and Athena

- **Status:** Accepted (M9)
- **Date:** 2026-11-14

## Context

Gold is a set of Parquet files on S3, written by dbt. Analysts should query **tables by name**, not file paths, as on AWS: the Glue Data Catalog describes the tables (columns and types, S3 location, file format), and Athena runs SQL on them. Glue stores no data; without it, Athena cannot read anything.

Constraints:
- the same emulator as everything else, Floci 2.4.0, whose Athena runs **DuckDB** (floci-duck), not Trino like real Athena (ADR 0001);
- one source of truth for the schema: the gold models already have enforced contracts (column names and types) and documented columns (M7);
- the success criterion of the module: the four analysis queries must give the same results through Athena and through DuckDB.

## Decision

1. **One S3 folder per table.** A Glue table (Hive style) is *everything under its prefix*: in M0, copying a file into the prefix doubled the results. dbt now writes each model to `s3://sme-lake/<layer>/<model>/<model>.parquet` (silver and gold), instead of nine files side by side in `gold/`.
2. **The catalog is built from dbt's manifest** (`scripts/register_glue.py`): database `sme_lake_gold`, one `EXTERNAL_TABLE` per gold model (Parquet SerDe), with the model's location, its contract types translated to Glue types (`varchar` → `string`, `integer` → `int`, `timestamp with time zone` → `timestamp`, decimals unchanged), the descriptions of the table and its columns, and a `contains_pii` parameter on the five personal-data columns. The script creates what is missing and updates the rest, so it can run after every build.
3. **Analysis SQL common to Trino and DuckDB** (`analytics/`): one query per analyst question, on the facts and dimensions. No DuckDB shortcut (`group by all`, `date - date`, `date - 90`); `date_diff('day', …)` and `interval '90' day` instead.
4. **Automatic comparison** (`scripts/check_athena.py`): each query runs through Athena (start, poll, read the results) and through an in-memory DuckDB whose views read the folders that Glue points to; the results are compared row by row as text. A query that Athena reports `SUCCEEDED` without any result is an error (gap found in M0).
5. **In the daily DAG:** `register_glue` then `check_athena`, after the dbt catalogue and before the run report. The Athena results go to `s3://sme-lake/athena-results/`, outside every table folder.

## What was proven (2026-11-14)

| Check | Result |
|---|---|
| Gold tables registered in Glue | ✅ 9 tables, 90 columns, 5 marked as personal data; the script checks the column count of every table against dbt. First run: `create_table`; next runs (from Airflow): `update_table`, both accepted by Floci |
| The four queries give the same results through Athena (Floci) and DuckDB | ✅ monthly revenue 15 rows, unpaid invoices 616, DSO 5, late payers 161; about 1.2 s per Athena query once floci-duck is warm |
| The guard against silent successes | ✅ a temporary query in DuckDB-only syntax (`from … select …`) came back `SUCCEEDED` with no result, and `check_athena.py` stopped with `SUCCEEDED without any result, not even a header` |
| Daily run | ✅ 8 tasks, `register_glue` and `check_athena` included |

## Limits (what this does not prove)

- **Athena vs DuckDB on Floci compares DuckDB with DuckDB.** The comparison proves the chain (Glue location and schema, files, the asynchronous API, the reading of results), not that real Athena (Trino) accepts this SQL or computes the same values. The queries were written to be portable, but **never run on Trino**. Rounding may differ (averages and divisions computed as floating point, then cast to decimals).
- **Glue column types:** declared from the contracts, but probably not used by Floci, which reads the Parquet schema itself (not verified). On AWS, Athena reads with the Glue types, and a mismatch with the files fails the query: the contracts keep both in line, provided `register_glue` runs after every change.
- **Column comments and parameters** (descriptions, `contains_pii`) are sent to Glue; whether Floci stores them was not checked. On AWS, access to personal-data columns would be restricted with Lake Formation, not with a parameter.
- **Tables removed from dbt stay in Glue:** the script does not delete them yet.
- **Inherited from ADR 0001:** result columns all reported as `varchar`, DuckDB's error messages instead of Athena's, `UNLOAD` and CTAS not available, no cost or speed figures that transfer to AWS.
- **Hive-style tables:** no transactions (a reader can see a table half rewritten), no row updates, no history. An open table format (Iceberg) would add them; it is an optional bonus.

## Alternatives considered

| Alternative | Why not |
|---|---|
| A Glue **crawler** that infers the schema from the files | It guesses types the contracts already state exactly, and it is one more service to run (and pay for on AWS). Not tested on Floci |
| **dbt-athena**, building the models in Athena | Uses CTAS, which Floci passes to DuckDB unchanged (rejected in M0); the models stay in dbt-duckdb |
| Athena DDL (`CREATE EXTERNAL TABLE … LOCATION …`) | Same issue: on Floci, only `SELECT` and `WITH` statements are handled as Athena queries. The Glue API is the reliable path |
| Reading the schema from the Parquet files | Possible, but loses the descriptions and the personal-data marks, and duplicates what the contracts already guarantee |

## Consequences

- Analysts (and any engine reading Glue) query `sme_lake_gold.<table>`; file paths stay an implementation detail.
- dbt stays the single place where a table and its columns are defined: Glue follows at every run.
- Before a real AWS deployment, the SQL must be run on Trino, and every limit above re-checked.

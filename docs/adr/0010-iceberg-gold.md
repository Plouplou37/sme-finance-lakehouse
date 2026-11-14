# ADR 0010: Publish the gold layer as Apache Iceberg tables

- **Status:** Accepted (M12)
- **Date:** 2026-11-14

## Context

Until M11, every table of the lake was a plain Parquet folder registered in Glue (ADR 0006): a table was *whatever files were under its prefix*. The ADRs listed what that costs, and gold, the layer analysts read, paid most of it:
- **non-atomic rewrites:** dbt replaced each gold file at every run; a reader arriving at that moment could see a missing or half-written table;
- **a partial Write-Audit-Publish:** the tests on gold itself ran after its files were written (ADR 0007);
- **no history:** a published number could not be read again as it was yesterday;
- **silent type changes:** a rewritten file could change a column's type under its readers; only dbt's contracts stood in the way.

An open table format fixes these by making a table an explicit list of files, versioned by a single pointer in a catalog. Apache Iceberg was chosen over Delta Lake because Athena, Trino, Spark, Flink, DuckDB and PyIceberg read it through the same catalogs, and Floci's Athena reads it (below).

Constraints found before building (research on 2026-11-14, then a spike in `spikes/m12/`):
- **No Iceberg runtime for Spark 4.2:** Iceberg 1.12.0 ships jars for Spark 3.5, 4.0 and 4.1 only.
- **dbt-duckdb cannot write Iceberg** (only dbt v2, still a pre-release, adds it); DuckDB itself writes Iceberg only through a REST catalog.
- **PyIceberg 0.12.0** writes Iceberg with Floci's Glue as the catalog; Floci's Athena reads Glue tables marked `table_type=ICEBERG`.

## Decision

1. **Gold only.** Bronze stays Parquet: Spark appends to it and its checkpoint already writes each batch once; moving it would need Spark 4.1. Silver stays Parquet: dbt reads it in the same run and nobody else does. Iceberg goes where people read while the pipeline writes.
2. **Write and audit in DuckDB, publish with PyIceberg.** dbt builds gold as tables inside the DuckDB warehouse and runs every gold test there. Only then does `scripts/publish_gold.py` publish each table, in one commit per table, as an Iceberg table of the Glue database `sme_lake_gold` (files under `s3://sme-lake/gold/<model>/data/` and `metadata/`). In the DAG, `publish_gold` replaces `register_glue` and runs only if `dbt_publish` passed.
3. **dbt stays the source of truth for the schema.** Added contract columns are added to the Iceberg schema (`union_by_name`); a changed type is refused by Iceberg and stops the run. Column descriptions become Iceberg column docs (Glue column comments); the model description and the personal-data columns become table properties.
4. **Housekeeping at every publication:** expire the snapshots older than 7 days (time travel stays possible for a week), then delete the files no kept snapshot references, if older than one day (the files of a commit in progress must survive).
5. **Readers follow the pointer.** `check_athena.py` reads each gold table in DuckDB with `iceberg_scan` on the metadata file Glue points to, never the folder: an Iceberg folder also holds the files of older snapshots.

## What was proven

| Check | Result |
|---|---|
| Create, then read three ways (spike 1) | ✅ `fct_invoices`: 1,359 rows and 4,511,288.67 total incl. VAT through PyIceberg, DuckDB `iceberg_scan` and Athena on Floci. Glue holds only the pointer (`metadata_location`, `previous_metadata_location`); S3 holds 1 data file and 4 metadata files |
| Atomic overwrite (spike 2) | ✅ Inside the transaction, the 5 new objects were already on S3 while Glue still pointed to the old version: DuckDB and Athena read the invoice as `open`. After the commit, both read `paid` |
| Time travel (spike 2) | ✅ PyIceberg by snapshot id and DuckDB on the old metadata file read the old version. ❌ Athena's `FOR VERSION AS OF` fails on Floci (DuckDB parser error); it works on AWS |
| Two writers (spike 3) | ✅ The second writer was refused (PyIceberg retried, found a conflict), then redid its work on the new version: both invoices paid, nothing lost. **Floci's Glue rejects an `UpdateTable` with a stale `VersionId`** (`ConcurrentModificationException`), the check that makes concurrent commits safe |
| Schema (spike 4) | ✅ dbt's 15 column descriptions of `dim_customer` reached Glue; an added column is refused until the schema evolves, then written; `valid_from` from timestamp to date refused (`Cannot change column type`) |
| Housekeeping (spike 5) | ✅ After 5 rewrites of a 53 KB table: 9 snapshots, 33 objects, 480 KB. `expire_snapshots` left every file in place (and wrote one more); our cleanup deleted 30 unreferenced files; the table read the same afterwards |
| In the pipeline | ✅ 9 gold tables published from the host, then by Airflow (8 tasks green in 1 min 19 s); the second publication changed no schema or property and kept 3 snapshots per table; the four analysis queries, which join several Iceberg tables, gave the same results through Athena and DuckDB as in M9 |

## Limits

- **One commit per table, not one for all of gold:** for a few seconds a reader can see a new `fct_invoices` with the previous `dim_customer`. Iceberg commits are per table; a multi-table commit needs a catalog that supports it.
- **Schema changes and data are two commits:** a new column can be seen empty for a few seconds. A column removed from a contract stays in the table, empty, until dropped on purpose.
- **PyIceberg 0.12 deletes no file when expiring snapshots,** and has no orphan-file removal: `publish_gold.py` does it. A bug in that code could delete live files; it only deletes files referenced by no kept snapshot and older than a day.
- **Every publication is a full overwrite** (option 1 of ADR 0008): about five new metadata files per table per run, and the old data files until the housekeeping. Replacing only the changed partitions would need gold models computed per day.
- **Personal data is marked per table, not per column, in Glue:** PyIceberg writes only its own column parameters. On AWS, access would be restricted with Lake Formation anyway.
- **Floci gaps:** no time travel in Athena SQL; the Glue pointer is the only catalog tested (no REST catalog).
- **Metadata overhead:** on a 53 KB table, about 24 % more bytes in metadata. Iceberg pays off with large tables, many readers or many writers, not at this size.

## When not to use it

- **Data that fits in one database:** a database already gives atomic commits, versions and cleanup (`VACUUM`) internally. At this volume, PostgreSQL would serve gold well; this ADR shows how the pieces of a lakehouse replace it at scale.
- **A table nobody reads while it is written,** or that is rewritten from scratch with no need for history: plain Parquet is simpler (silver and bronze here).

## Alternatives considered

| Alternative | Why not |
|---|---|
| Iceberg on every layer | Bronze would need Spark 4.1 (no Iceberg jar for 4.2) and gains little; silver has no outside reader |
| Delta Lake | Same principle; Iceberg has the wider engine and catalog support around Athena and Glue |
| A REST catalog (Polaris, Lakekeeper) so that DuckDB writes Iceberg itself | One more service; Glue is the catalog Athena already uses, and it proved safe for concurrent commits on Floci |
| dbt v2 with Iceberg catalogs | Still a pre-release, against the 14-day version rule |
| Write-Audit-Publish with Iceberg branches | Not needed: gold is fully tested in DuckDB before anything is written to S3 |
| Amazon S3 Tables | Emulated by Floci as metadata only |

# ADR 0009: Check every change with GitHub Actions, on a generated sample and without services

- **Status:** Accepted (M10)
- **Date:** 2026-11-14

## Context

Every change to the models, the SQL or the Python code can break a number that analysts read. The repository is on GitHub, and `main` should only receive changes that pass the lint, the tests and a full dbt build.

A CI that starts the whole stack (Floci, Redpanda, Spark, Airflow) would take minutes, depend on services that can be slow to start, and need credentials, even fake ones. A CI that fails for no real reason ends up being ignored.

## Decision

1. **GitHub Actions, one job named `checks`,** on every pull request and every change on `main` (`.github/workflows/ci.yml`):
   1. `uv sync --locked`: the exact packages of `uv.lock`, or an error if the lock is out of date;
   2. `ruff check .`: Python lint (errors, imports, modern syntax, common bug patterns);
   3. `sqlfluff lint analytics`: the four analysis queries parsed with **Trino's grammar**, the engine of real Athena;
   4. `pytest`: the generator's tests;
   5. the generator, then `generator/local_lake.py`: a bronze layer written in a local folder, with the same columns and folders as Spark's;
   6. `dbt build --target ci` with `LAKE_ROOT=data/ci-lake`: every model and test, on a local DuckDB file, with no S3 and no credentials.
2. **Pinned like the images:** runner `ubuntu-24.04` (not `ubuntu-latest`), actions pinned by commit SHA (a tag can be moved, a commit cannot), uv 0.8.11 as on the development machine, with its cache.
3. **Least privilege:** the job's token can only read the repository (`permissions: contents: read`), and the checkout does not keep it (`persist-credentials: false`).
4. **A rule set on `main`:** pull request required, `checks` required to pass, no force push, no deletion, and **no bypass**, not even for the repository's owner.

## What was proven (M10)

| Check | Result |
|---|---|
| Lint, tests and dbt on a sample, with no service | ✅ 13 tests, then 19 models and 54 tests on 2,578 generated events (72 pass, 1 warning, as on the real lake), in about 25 s |
| A broken change is blocked | ✅ a pull request breaking the `dim_company` contract failed with dbt's mismatch table (`DATE` instead of `TIMESTAMP WITH TIME ZONE`, 22 nodes skipped); the merge button stayed greyed out. Closed without merging |
| `main` is green | ✅ after merging the module's pull request |
| The analysis queries are valid Trino syntax | ✅ all four parse with sqlfluff's Trino grammar. A query in DuckDB-only syntax was not submitted to check that it is refused |

## Limits: what a green CI does not prove

- **Orchestration:** the Airflow DAG is linted but never loaded (Airflow is not installed in the CI). The M8 bugs (start-up order, volume permissions, a missing date variable) would have passed.
- **The real services:** Kafka, Spark, S3 on Floci, Glue and Athena do not run. A local disk is not S3 (folders must exist on a disk, not on S3).
- **The real engine:** Trino parses nothing here; sqlfluff checks the grammar, not the results.
- **The day's data:** the sample is the generator's default dataset. New anomalies or a schema change from the application are caught by the pipeline's own tests at each run (ADR 0007), not by the CI.
- **Volume:** 2,578 events say nothing about memory, file sizes or time at 100 million rows.
- **Environments can drift:** the CI installs from `uv.lock`, the Airflow image from its own pinned list (ADR 0008).
- **Not done yet:** loading the DAG in a test, linting the dbt models, dbt unit tests, building only what a pull request changed against production artifacts (Slim CI), a nightly run of the full stack.

## When not to use it

- **Code hosted elsewhere:** GitLab CI or another runner applies the same pattern.
- **Integration tests of the whole stack:** better as a scheduled run (nightly) on a staging environment than on every pull request.

## Alternatives considered

| Alternative | Why not |
|---|---|
| The whole Compose stack in the CI | Minutes per run, more failures unrelated to the change, credentials to manage; kept as an idea for a nightly run |
| Pre-commit hooks only | Run on the developer's machine and can be skipped; they complement a CI, they do not replace it |
| GitLab CI, Jenkins | The repository is on GitHub, where Actions is built in |
| dbt Cloud CI | Another hosted service and account; the same commands run in Actions |

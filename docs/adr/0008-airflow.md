# ADR 0008: Orchestrate the daily pipeline with Airflow, which orchestrates and does not compute

- **Status:** Accepted (M8, extended in M9)
- **Date:** 2026-11-14

## Context

The pipeline has eight steps that depend on each other: check the services, ingest Kafka into bronze, build and test, publish, document, register in Glue, check through Athena, report. It must run every day, retry what fails for a passing reason, stop before publishing wrong data, alert with the cause, and be replayed or run for past dates without creating duplicates.

Constraints: the services run in Docker Compose on one machine (ADR 0003), and no container may get the Docker socket.

## Decision

1. **Apache Airflow 3.3.2 with the LocalExecutor:** the scheduler runs the tasks itself, as processes. The official Compose file (Celery, Redis, workers) was cut down to PostgreSQL 16.14, a one-shot `airflow-init`, the api-server, the scheduler and the dag-processor, in the `pipeline` profile.
2. **Airflow orchestrates, it does not compute:** Spark runs as a **Spark Connect** server and the ingestion task sends it the job over the network (ADR 0005); dbt and DuckDB are installed in our Airflow image, each tool in its own virtual environment so they cannot break Airflow's dependencies; the Glue and Athena scripts run as subprocesses. No `DockerOperator`, so no Docker socket.
3. **The `sme_lake_pipeline` DAG:** daily, `catchup=False`, at most one active run (DuckDB has a single writer, Spark a single checkpoint):

   ```text
   check_sources -> ingest_bronze -> dbt_audit -> dbt_publish -> publish_catalog
                 -> register_glue -> check_athena -> run_report
   ```

4. **Each run processes the whole dataset, and every task is idempotent.** A run can be replayed, and any run repairs the whole history. The run's date only names its report.
5. **Retries for passing failures only:** two retries 30 s apart by default; none for the dbt tasks and the Athena check, whose failures come back the same way.
6. **The publication barrier lives in the DAG:** `dbt_publish` starts only if `dbt_audit` passed (ADR 0007).
7. **Alerts:** every failed attempt appends one JSON line to `data/alerts/alerts.log` (a local stand-in for a chat or paging webhook) with the task, the run, the attempt, a level (`retry`: Airflow will try again; `failure`: the run stops) and the cause. For dbt, the cause names the failed models or tests, read from dbt's `run_results.json`.
8. **A report per run** (`data/reports/`): duration, dbt results of both passes, warnings, rows per entity, reconciliation with the manifest.

## What was proven (M8, M9)

| Check | Result |
|---|---|
| End to end | ✅ 8 tasks green in the daily run |
| Replay without duplicates | ✅ a run cleared and run again gave the same report; 2,578 bronze rows and 2,519 distinct events every time |
| Backfill | ✅ 3 past days, run one after the other, same counts |
| Outage, retry, recovery | ✅ Redpanda stopped: `check_sources` failed with `redpanda:9092 unreachable`, a `retry` alert was written; Redpanda restarted, the next attempt passed |
| Blocking test | ✅ threshold lowered: `dbt_audit` failed, `dbt_publish` never started, silver and gold on S3 untouched; alert `quarantine_share_error (fail); publication blocked` |
| Report | ✅ one per successful run, scheduled and backfilled ones included |

## Limits

- **The run's date is not used:** the backfill proves the mechanism (past runs created and run one by one, without duplicates), not the reprocessing of one given day. Processing one day per run (incremental by date partition, with a window for late events, and state tables merged on their key) is the target at scale; it needs a producer that publishes day by day and tables whose partitions can be replaced.
- **dbt runs inside the scheduler's container:** fine at this volume; a heavy model would compete with scheduling.
- **Alerts go to a file,** not to a pager or a chat; no alert when data is late (SLA).
- **One scheduler, one machine:** no high availability.
- **Image hygiene:** known vulnerabilities in the base image are not triaged, and the image's own Python packages are pinned (pandas only below 3) but their dependencies are not locked.
- **The DAG is not loaded in the CI** (ADR 0009): an import error would be seen only in Airflow.
- **Start-up order matters:** each task asks the api-server for permission to start; a run started while the api-server was booting was killed, which led to the health-check dependency (ADR 0003).

## When not to use it

- **One script once a day, no dependencies:** cron or a scheduled CI job is enough.
- **Event-driven or continuous processing:** Airflow schedules batches; a stream runs on its own (ADR 0005).
- **No one to operate it:** a managed service (Amazon MWAA, Google Cloud Composer) runs the same DAGs.

## Alternatives considered

| Alternative | Why not |
|---|---|
| cron and the Makefile | No dependencies between steps, no retries, no backfill, no history |
| `DockerOperator` (one container per task) | Needs the Docker socket, root on the host |
| CeleryExecutor (official Compose file) | Redis and workers for one machine and one run at a time |
| Dagster, Prefect | Not evaluated: Airflow was chosen as the most established option |

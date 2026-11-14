"""The whole data pipeline, every day: checks -> Kafka to bronze -> dbt audit -> dbt publish -> catalogue -> report.

Write-Audit-Publish: dbt runs twice.
- dbt_audit builds staging and intermediate (tables in the DuckDB file, never read by the end users) and runs
  their tests, including the blocking share of rejected events;
- dbt_publish builds silver, gold and quality (the Parquet files on S3, what the users read) and their tests.
If a blocking test fails in the audit, Airflow never starts dbt_publish: the published files stay as they were.
One `dbt build` alone is not enough: dbt only skips the models whose ancestors include every model the failed
test reads, so dim_customer (customers only) was published in spite of a failed test on all three entities.

Airflow only orchestrates; the work happens elsewhere:
- the ingestion job is sent over the network to the Spark Connect server (spark-connect service);
- dbt runs in its own virtual environment inside this image, with DuckDB as its engine.
Each run processes everything available (the whole generated dataset): every task is idempotent, so
running a date again changes nothing (Spark's checkpoint, dbt's deduplication on event_id, full rebuilds).
The logical date of the run is only used to name its report.

Alerts: every failed attempt (retried or final) appends one line to data/alerts/alerts.log, a local stand-in
for a Slack or e-mail webhook. Report: every successful run writes data/reports/<date>__<run>.md.
"""

import json
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

PROJECT = "/opt/project"  # the repository folders mounted into the container (docker-compose.yml)
DBT = "/home/airflow/dbt-venv/bin/dbt"
# Compiled SQL and run results of each dbt build, read by the alert and the report: one folder per task, so the
# publication does not overwrite the results of the audit.
DBT_TARGETS = {"dbt_audit": "/opt/airflow/dbt-target/audit", "dbt_publish": "/opt/airflow/dbt-target/publish"}
CATALOG_DIR = f"{PROJECT}/data/catalog"  # the catalogue, apart: dbt docs generate rewrites run_results.json
DBT_ARGS = "--project-dir dbt --profiles-dir dbt --log-path /opt/airflow/logs/dbt"
SPARK_CLIENT_PYTHON = "/home/airflow/spark-client-venv/bin/python"
REPORT_PYTHON = "/home/airflow/dbt-venv/bin/python"  # this environment has DuckDB, to read the warehouse
ALERTS_FILE = Path(PROJECT) / "data" / "alerts" / "alerts.log"


def failed_dbt_nodes(task_id: str) -> str | None:
    """The dbt models and tests that failed in this task's dbt run, and what was not published because of
    them, read from its run_results.json."""
    path = Path(DBT_TARGETS[task_id]) / "run_results.json"
    if not path.exists():
        return None
    results = json.loads(path.read_text(encoding="utf-8"))["results"]
    failed = [f"{r['unique_id']} ({r['status']})" for r in results if r["status"] in ("error", "fail")]
    if not failed:
        return None
    if task_id == "dbt_audit":
        return f"{', '.join(failed)}; publication blocked: silver and gold not rewritten"
    skipped = sum(1 for r in results if r["status"] == "skipped")
    return f"{', '.join(failed)}; {skipped} downstream nodes skipped (not published)"


def write_alert(context, level: str) -> None:
    """Append one JSON line per failed attempt: which task, which run, which attempt, and the cause."""
    ti = context["ti"]
    exception = context.get("exception")
    if exception is None:
        # The task's code never raised: Airflow itself saw it fail (not started, killed, lost). Seen when the
        # scheduler started a task before the API server was ready: no exception, and the alert said "None".
        cause = "no error from the task's code: it did not start or was killed; see the scheduler's log"
    else:
        cause = str(exception)
    if ti.task_id in DBT_TARGETS:
        cause = failed_dbt_nodes(ti.task_id) or cause  # name the model or test that failed, not just "exit code 1"
    alert = {
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "level": level,  # "retry": Airflow will try again; "failure": no attempt left, the run stops here
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context.get("run_id"),
        "try_number": ti.try_number,
        "cause": cause,
    }
    ALERTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with ALERTS_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(alert) + "\n")


def alert_on_retry(context) -> None:
    write_alert(context, "retry")


def alert_on_failure(context) -> None:
    write_alert(context, "failure")


@task
def check_sources() -> None:
    """Fail fast, with a clear cause, if a service the pipeline needs cannot be reached."""
    for host, port in [("floci", 4566), ("redpanda", 9092), ("spark-connect", 15002)]:
        try:
            with socket.create_connection((host, port), timeout=5):
                print(f"{host}:{port} reachable")
        except OSError as error:  # the bare error does not say which service: the alert must name it
            raise ConnectionError(f"{host}:{port} unreachable ({error})") from error
    manifest = Path(PROJECT) / "data" / "generated" / "manifest.json"
    if not manifest.exists():
        raise FileNotFoundError(f"{manifest} is missing: run the generator first")


@dag(
    dag_id="sme_lake_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,  # no automatic catch-up of missed days; past dates are run on purpose (backfill)
    max_active_runs=1,  # one run at a time: DuckDB has a single writer, Spark a single checkpoint
    default_args={
        "retries": 2,  # transient failures (a service restarting) get two more attempts...
        "retry_delay": timedelta(seconds=30),
        "on_retry_callback": alert_on_retry,
        "on_failure_callback": alert_on_failure,
    },
    params={"quarantine_error_share": 0.05},  # lower it when triggering, to make the blocking test fail on purpose
    tags=["sme-lake"],
)
def sme_lake_pipeline():
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"{SPARK_CLIENT_PYTHON} {PROJECT}/spark/jobs/ingest_bronze.py",
    )

    def dbt_build(task_id: str, selection: str) -> BashOperator:
        target = DBT_TARGETS[task_id]
        return BashOperator(
            task_id=task_id,
            bash_command=(
                # Remove the previous results first: if dbt crashes before writing new ones, the alert must not
                # blame the models or tests of an older run.
                f"rm -f {target}/run_results.json && "
                f"cd {PROJECT} && {DBT} build {DBT_ARGS} --target-path {target} {selection}"
            ),
            retries=0,  # ...but a failed test fails again the same way: retrying it would only delay the alert
        )

    dbt_audit = dbt_build(
        "dbt_audit",
        # cautious: only the tests whose models are ALL selected. A test that also reads silver (bronze = silver
        # + quarantine) waits for the publication, instead of checking last run's silver files.
        # (A folder name selects every model under models/<folder>.)
        "--select staging intermediate --indirect-selection cautious "
        "--vars '{\"quarantine_error_share\": {{ params.quarantine_error_share }}}'",
    )
    # The default (eager) brings the tests that read a silver, gold or quality model, even if they also read
    # staging, already built by the audit.
    dbt_publish = dbt_build("dbt_publish", "--select silver gold quality")

    publish_catalog = BashOperator(
        task_id="publish_catalog",
        # The site (index.html, manifest.json, catalog.json) lands in data/catalog/ on the host. Glue joins in M9.
        bash_command=f"cd {PROJECT} && {DBT} docs generate {DBT_ARGS} --target-path {CATALOG_DIR}",
    )

    run_report = BashOperator(
        task_id="run_report",
        bash_command=(
            f"{REPORT_PYTHON} {PROJECT}/airflow/scripts/run_report.py "
            f"--dbt-target {DBT_TARGETS['dbt_audit']} --dbt-target {DBT_TARGETS['dbt_publish']} "
            # Not {{ ds }}: a run triggered from the CLI or the API can have no logical date (Airflow 3), and
            # then {{ ds }} does not exist. run_after always exists.
            "--logical-date '{{ dag_run.logical_date }}' --run-after '{{ dag_run.run_after }}' "
            "--run-id '{{ run_id }}' --started-at '{{ dag_run.start_date }}'"
        ),
    )

    check_sources() >> ingest_bronze >> dbt_audit >> dbt_publish >> publish_catalog >> run_report


sme_lake_pipeline()

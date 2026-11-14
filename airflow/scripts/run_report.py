"""End-of-run report of the pipeline (M8): rows processed, rejections by reason, duration, test results.

Called by the last task of the Airflow DAG, with the Python of dbt's virtual environment (it has DuckDB).
Reads the warehouse (read-only) and dbt's run_results.json, and writes data/reports/<date>__<run>.md.
"""

import argparse
import json
import os
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import duckdb

REPORTS_DIR = Path("/opt/project/data/reports")


def main() -> None:
    parser = argparse.ArgumentParser()
    # Once per dbt build of the run (audit, then publication), in that order.
    parser.add_argument("--dbt-target", required=True, action="append")
    parser.add_argument("--logical-date", required=True)  # "None" when the run was triggered without a date
    parser.add_argument("--run-after", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--started-at", required=True)
    args = parser.parse_args()
    run_date = (args.logical_date if args.logical_date != "None" else args.run_after)[:10]

    warehouse = duckdb.connect(os.environ["DBT_DUCKDB_PATH"], read_only=True)
    bronze_rows, distinct_events = warehouse.execute(
        "select count(*), count(distinct event_id) from staging.stg_events"
    ).fetchone()
    by_type = warehouse.execute(
        """
        select 'customers' as entity, count(*) filter (where rejection_reason is null), count(*) filter (where rejection_reason is not null)
        from intermediate.int_customer_events
        union all
        select 'invoices', count(*) filter (where rejection_reason is null), count(*) filter (where rejection_reason is not null)
        from intermediate.int_invoice_events
        union all
        select 'payments', count(*) filter (where rejection_reason is null), count(*) filter (where rejection_reason is not null)
        from intermediate.int_payment_events
        """
    ).fetchall()
    reconciliation = warehouse.execute(
        "select check_name, expected, detected, status from quality.quality_reconciliation order by check_name"
    ).fetchall()
    warehouse.close()

    results, dbt_seconds = [], 0.0
    for target in args.dbt_target:
        run_results = json.loads((Path(target) / "run_results.json").read_text(encoding="utf-8"))
        results += run_results["results"]
        dbt_seconds += run_results["elapsed_time"]
    statuses = Counter(result["status"] for result in results)
    # Warnings do not stop the run and wake nobody up: they belong in this report, by name.
    warnings = [result["unique_id"] for result in results if result["status"] == "warn"]

    started_at = datetime.fromisoformat(args.started_at)
    duration = datetime.now(UTC) - started_at

    lines = [
        f"# Pipeline run {run_date}",
        "",
        f"- Run: `{args.run_id}`, logical date: {args.logical_date}",
        f"- Started: {started_at.isoformat(timespec='seconds')}, duration until the report: {int(duration.total_seconds())} s",
        f"- dbt (audit + publication): {dict(sorted(statuses.items()))} in {dbt_seconds:.1f} s",
        f"- Warnings to look at: {', '.join(warnings) if warnings else 'none'}",
        "",
        "## Rows processed",
        "",
        f"- Bronze: {bronze_rows} rows, {distinct_events} distinct events ({bronze_rows - distinct_events} duplicates removed)",
        "",
        "| Entity | Valid events (silver) | Rejected (quarantine) |",
        "|---|---|---|",
        *[f"| {entity} | {valid} | {rejected} |" for entity, valid, rejected in by_type],
        "",
        "## Reconciliation with the generator's manifest",
        "",
        "| Check | Expected | Detected | Status |",
        "|---|---|---|---|",
        *[f"| {name} | {expected} | {'' if detected is None else detected} | {status} |" for name, expected, detected, status in reconciliation],
        "",
    ]

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", args.run_id)  # run ids contain ":" and "+", not allowed on Windows
    path = REPORTS_DIR / f"{run_date}__{safe_run_id}.md"
    path.write_text("\n".join(lines), encoding="utf-8", newline="\n")
    print(f"Report written: {path}")
    print("\n".join(lines))


if __name__ == "__main__":
    main()

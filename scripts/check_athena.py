"""Run the analysis queries through Athena and through DuckDB, and check that both give the same results (M9).

Each file of analytics/ is run twice, with the same SQL text:
- by Athena (emulated by Floci), which finds the tables in the Glue catalog (scripts/register_glue.py);
- by DuckDB, on views named like the Glue tables and reading the same S3 folders, taken from Glue too.
The results are compared row by row, as text (Athena returns every value as text). Any difference, or a
query that Athena reports as SUCCEEDED without a result, makes the script fail.

Run from the repository root, after register_glue.py:
    uv run --env-file .env python scripts/check_athena.py
"""

import os
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import boto3
import duckdb

from register_glue import DATABASE  # run as a script, scripts/ is on the import path

QUERIES_DIR = Path(__file__).resolve().parent.parent / "analytics"
# Athena writes every result as a CSV file on S3. Kept out of the table folders (silver/, gold/):
# any file under a table's prefix becomes part of the table (M0).
RESULTS_LOCATION = "s3://sme-lake/athena-results/"
TIMEOUT_SECONDS = 120


def run_on_athena(athena, sql: str) -> tuple[list, list]:
    """Start the query, wait for it, read its results. Athena is asynchronous: a query on a real lake can
    scan terabytes for minutes, so the caller gets an id and asks for the status."""
    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": RESULTS_LOCATION},
    )["QueryExecutionId"]

    deadline = time.monotonic() + TIMEOUT_SECONDS
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.monotonic() > deadline:
            raise SystemExit(f"Athena query {query_id} still {status['State']} after {TIMEOUT_SECONDS} s")
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise SystemExit(f"Athena query {query_id} {status['State']}: {status.get('StateChangeReason')}")

    rows = [
        [cell.get("VarCharValue") for cell in row["Data"]]  # a NULL has no VarCharValue
        for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=query_id)
        for row in page["ResultSet"]["Rows"]
    ]
    # Seen in M0: Floci can report SUCCEEDED for a query it never ran, with no result at all. A real
    # result always has at least its header row.
    if not rows:
        raise SystemExit(f"Athena query {query_id} SUCCEEDED without any result, not even a header")
    header, data = rows[0], rows[1:]  # the first row holds the column names
    return header, [[None if value == "" else value for value in row] for row in data]


def as_text(value) -> str | None:
    """A DuckDB value written the way Athena writes it."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value, "f")  # 1234.50, never 1.2345E+3
    if isinstance(value, (date, datetime)):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    return str(value)


def duckdb_on_glue_tables(glue) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB with one view per Glue table, reading the folder that Glue points to."""
    endpoint = os.environ["AWS_ENDPOINT_URL"]
    connection = duckdb.connect()
    connection.execute("INSTALL httpfs; LOAD httpfs; SET TimeZone = 'UTC'")
    connection.execute(
        f"""
        CREATE SECRET floci (
            TYPE s3,
            KEY_ID '{os.environ['AWS_ACCESS_KEY_ID']}',
            SECRET '{os.environ['AWS_SECRET_ACCESS_KEY']}',
            REGION '{os.environ['AWS_DEFAULT_REGION']}',
            ENDPOINT '{endpoint.split('://', 1)[-1]}',
            URL_STYLE 'path',
            USE_SSL {str(endpoint.startswith('https')).lower()}
        )
        """
    )
    connection.execute(f"CREATE SCHEMA {DATABASE}")
    for page in glue.get_paginator("get_tables").paginate(DatabaseName=DATABASE):
        for table in page["TableList"]:
            location = table["StorageDescriptor"]["Location"].rstrip("/")
            connection.execute(
                f"CREATE VIEW {DATABASE}.{table['Name']} AS SELECT * FROM read_parquet('{location}/*.parquet')"
            )
    return connection


def main() -> None:
    athena = boto3.client("athena")
    glue = boto3.client("glue")
    duck = duckdb_on_glue_tables(glue)

    files = sorted(QUERIES_DIR.glob("*.sql"))
    if not files:
        raise SystemExit(f"No query in {QUERIES_DIR}")

    failures = []
    for path in files:
        sql = path.read_text(encoding="utf-8")
        started = time.monotonic()
        athena_header, athena_rows = run_on_athena(athena, sql)
        athena_seconds = time.monotonic() - started

        result = duck.execute(sql)
        duck_header = [column[0] for column in result.description]
        duck_rows = [[as_text(value) for value in row] for row in result.fetchall()]

        problems = []
        if athena_header != duck_header:
            problems.append(f"columns differ: Athena {athena_header}, DuckDB {duck_header}")
        if not duck_rows:
            problems.append("no row: the comparison would prove nothing")
        if athena_rows != duck_rows:
            different = sum(1 for a, d in zip(athena_rows, duck_rows) if a != d)
            first = next(((a, d) for a, d in zip(athena_rows, duck_rows) if a != d), None)
            problems.append(
                f"rows differ: Athena {len(athena_rows)}, DuckDB {len(duck_rows)}, {different} different; "
                f"first difference: Athena {first[0] if first else None}, DuckDB {first[1] if first else None}"
            )

        status = "OK" if not problems else "DIFFERENT"
        print(f"{status}: {path.name}: {len(athena_rows)} rows, {len(athena_header)} columns "
              f"(Athena {athena_seconds:.1f} s); first row: {athena_rows[0] if athena_rows else None}")
        for problem in problems:
            print(f"    {problem}")
        if problems:
            failures.append(path.name)

    if failures:
        raise SystemExit(f"Athena and DuckDB disagree on: {', '.join(failures)}")
    print(f"OK: the {len(files)} queries give the same results through Athena and DuckDB")


if __name__ == "__main__":
    main()

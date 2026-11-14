"""Shared setup for the Iceberg spike (M12): the Glue catalog on Floci, DuckDB on S3, Athena.

The spike writes into its own Glue database and its own S3 prefix, so the real gold tables
(sme_lake_gold, s3://sme-lake/gold/) are never touched.
"""

import os
import time
from decimal import Decimal

import boto3
import duckdb
from pyiceberg.catalog import Catalog, load_catalog

NAMESPACE = "sme_lake_spike"  # a Glue database of its own
WAREHOUSE = "s3://sme-lake/spike-iceberg"  # outside bronze/, silver/ and gold/
RESULTS_LOCATION = "s3://sme-lake/athena-results/"


def gold_parquet(model: str) -> str:
    """Where dbt writes a gold model today: the source of the spike's tables."""
    return f"s3://sme-lake/gold/{model}/{model}.parquet"


def glue_catalog() -> Catalog:
    """PyIceberg's Glue catalog, pointed at Floci for both Glue (the pointer) and S3 (the files)."""
    endpoint = os.environ["AWS_ENDPOINT_URL"]
    credentials = {
        "access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
        "secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "region": os.environ["AWS_DEFAULT_REGION"],
    }
    properties = {"type": "glue", "glue.endpoint": endpoint, "s3.endpoint": endpoint}
    for key, value in credentials.items():
        properties[f"glue.{key}"] = value
        properties[f"s3.{key}"] = value
    return load_catalog("floci", **properties)


def duckdb_on_s3(iceberg: bool = False) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB that reads s3:// on Floci (and Iceberg tables if asked)."""
    endpoint = os.environ["AWS_ENDPOINT_URL"]
    connection = duckdb.connect()
    connection.execute("INSTALL httpfs; LOAD httpfs; SET TimeZone = 'UTC'")
    if iceberg:
        connection.execute("INSTALL iceberg; LOAD iceberg")
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
    return connection


def run_on_athena(sql: str, database: str = NAMESPACE, timeout: int = 120) -> list[list]:
    """Start, poll, read (as in scripts/check_athena.py). Returns the rows without the header."""
    athena = boto3.client("athena")
    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": RESULTS_LOCATION},
    )["QueryExecutionId"]
    deadline = time.monotonic() + timeout
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.monotonic() > deadline:
            raise SystemExit(f"Athena query still {status['State']} after {timeout} s")
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise SystemExit(f"Athena query {status['State']}: {status.get('StateChangeReason')}")
    rows = [
        [cell.get("VarCharValue") for cell in row["Data"]]
        for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=query_id)
        for row in page["ResultSet"]["Rows"]
    ]
    if not rows:  # Floci can report SUCCEEDED for a query it never ran (M0)
        raise SystemExit("Athena query SUCCEEDED without any result, not even a header")
    return rows[1:]


def as_text(value) -> str | None:
    """Values written the way Athena returns them, to compare readers."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def glue_pointer(table_name: str, database: str = NAMESPACE) -> dict:
    """The Iceberg pointer as Glue holds it: the current and previous metadata files."""
    parameters = boto3.client("glue").get_table(DatabaseName=database, Name=table_name)["Table"]["Parameters"]
    return {key: parameters.get(key) for key in ("metadata_location", "previous_metadata_location")}


def list_files(location: str) -> list[tuple[str, int]]:
    """Every object under an s3:// location, with its size: what is physically there."""
    bucket, _, prefix = location.removeprefix("s3://").partition("/")
    s3 = boto3.client("s3")
    return [
        (item["Key"], item["Size"])
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/")
        for item in page.get("Contents", [])
    ]

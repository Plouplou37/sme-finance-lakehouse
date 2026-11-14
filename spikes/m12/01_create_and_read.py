"""Iceberg spike (M12), step 1: publish one gold table as Iceberg in Floci's Glue, then read it three ways.

1. Read fct_invoices as dbt wrote it (Parquet on S3): the reference count and total.
2. Create the Iceberg table with PyIceberg (Glue database sme_lake_spike, files under
   s3://sme-lake/spike-iceberg/fct_invoices/) and overwrite its content: one new snapshot per run.
3. Show what Glue stores (a pointer to a metadata file) and what is on S3 (metadata/ and data/).
4. Read the table back through PyIceberg, DuckDB (iceberg_scan) and Athena on Floci, and compare.

Run from the repository root, with the lake up (make up) and gold built at least once:
    uv run --env-file .env python spikes/m12/01_create_and_read.py
"""

import boto3
import pyarrow.compute as pc
from common import (
    NAMESPACE,
    WAREHOUSE,
    as_text,
    duckdb_on_s3,
    glue_catalog,
    gold_parquet,
    list_files,
    run_on_athena,
)
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

MODEL = "fct_invoices"


def main() -> None:
    duck = duckdb_on_s3(iceberg=True)

    # 1. The reference: gold as dbt publishes it today.
    source = duck.execute(f"SELECT * FROM read_parquet('{gold_parquet(MODEL)}')").to_arrow_table()
    expected = (str(source.num_rows), as_text(pc.sum(source["amount_incl_vat"]).as_py()))
    print(f"source (dbt's Parquet): {expected[0]} rows, total incl. VAT {expected[1]}, {source.num_columns} columns")

    # 2. Create the table if needed, then replace its content.
    catalog = glue_catalog()
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"Glue database created: {NAMESPACE}")
    except NamespaceAlreadyExistsError:
        print(f"Glue database already exists: {NAMESPACE}")
    identifier = f"{NAMESPACE}.{MODEL}"
    try:
        table = catalog.load_table(identifier)
        print(f"Iceberg table already exists: {identifier}")
    except NoSuchTableError:
        table = catalog.create_table(identifier, schema=source.schema, location=f"{WAREHOUSE}/{MODEL}")
        print(f"Iceberg table created: {identifier}")
    table.overwrite(source)

    # 3. What Glue and S3 hold now. Reload the table as any reader would: through the catalog.
    table = catalog.load_table(identifier)
    glue_table = boto3.client("glue").get_table(DatabaseName=NAMESPACE, Name=MODEL)["Table"]
    print(f"Glue: TableType={glue_table.get('TableType')}, Parameters={glue_table.get('Parameters')}")
    print(f"current snapshot: {table.current_snapshot().snapshot_id}, snapshots kept: {len(table.snapshots())}")
    files = list_files(table.location())
    print(f"S3 under {table.location()}: {len(files)} objects")
    for key, size in files:
        print(f"    {size:>9,} B  {key}")

    # 4. Read it back three ways.
    scanned = table.scan().to_arrow()
    readers = {"PyIceberg": (str(scanned.num_rows), as_text(pc.sum(scanned["amount_incl_vat"]).as_py()))}
    count, total = duck.execute(
        f"SELECT count(*), sum(amount_incl_vat) FROM iceberg_scan('{table.metadata_location}')"
    ).fetchone()
    readers["DuckDB iceberg_scan"] = (str(count), as_text(total))
    athena_rows = run_on_athena(f"SELECT count(*), sum(amount_incl_vat) FROM {MODEL}")
    readers["Athena (Floci)"] = tuple(athena_rows[0])

    different = False
    for reader, result in readers.items():
        same = result == expected
        different |= not same
        print(f"{'OK' if same else 'DIFFERENT'}: {reader}: {result[0]} rows, total {result[1]}")
    if different:
        raise SystemExit("The readers disagree with the source")
    print(f"OK: {identifier} is an Iceberg table in Glue, read identically by PyIceberg, DuckDB and Athena")
    print(f"(warehouse prefix: {WAREHOUSE})")


if __name__ == "__main__":
    main()

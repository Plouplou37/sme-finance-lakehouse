"""Register the gold tables in the Glue Data Catalog (M9), so that Athena can query them by name.

Glue stores no data: a Glue table is a description (columns and their types, the S3 prefix holding the
files, the file format) that lets Athena, or any engine reading Glue, query those files as a table.
Everything comes from dbt's manifest, nothing is written twice:
- the S3 location of each gold model (its `location` config: one folder per table);
- its columns and their types, which the model contracts (M7) guarantee to match the Parquet files;
- the descriptions of the tables and columns, and the columns marked as personal data.

Idempotent: creates the database and the tables that are missing, updates the others. Tables removed
from dbt are not removed from Glue (not needed so far). Run it after each dbt build, from the repository root:
    uv run --env-file .env python scripts/register_glue.py [--manifest data/catalog/manifest.json]
The default manifest is the one written by the Airflow task publish_catalog; after a dbt run by hand,
use dbt/target/manifest.json.
"""

import argparse
import json
import posixpath
from pathlib import Path

import boto3

DATABASE = "sme_lake_gold"
LAYER = "gold"  # the folder of models/ whose models are registered

# DuckDB types used in the gold contracts -> Glue (Hive) types, the ones Athena reads.
GLUE_TYPES = {
    "varchar": "string",
    "integer": "int",
    "bigint": "bigint",
    "boolean": "boolean",
    "date": "date",
    "double": "double",
    "timestamp": "timestamp",
    # Parquet stores it as an instant in UTC; Athena reads it as a plain timestamp, in UTC.
    "timestamp with time zone": "timestamp",
}
PARQUET_FORMAT = {
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
}


def glue_type(duckdb_type: str) -> str:
    duckdb_type = duckdb_type.strip().lower()
    if duckdb_type.startswith("decimal"):
        return duckdb_type.replace(" ", "")  # "decimal(18, 2)" -> "decimal(18,2)"
    if duckdb_type not in GLUE_TYPES:
        raise SystemExit(f"No Glue type for the dbt type {duckdb_type!r}: add it to GLUE_TYPES")
    return GLUE_TYPES[duckdb_type]


def table_input(model: dict) -> dict:
    """The Glue description of one dbt model."""
    location = model["config"].get("location")
    if not location:
        raise SystemExit(f"{model['name']} has no location: is it an external model?")
    # dbt writes one file, s3://sme-lake/gold/<model>/<model>.parquet; the Glue table is its folder,
    # and every file in that folder is part of the table (seen in M0).
    folder = posixpath.dirname(location) + "/"

    columns = []
    for column in model["columns"].values():
        if not column.get("data_type"):
            raise SystemExit(f"{model['name']}.{column['name']} has no data_type in its contract")
        meta = {**column.get("meta", {}), **column.get("config", {}).get("meta", {})}
        columns.append(
            {
                "Name": column["name"],
                "Type": glue_type(column["data_type"]),
                "Comment": column.get("description", "")[:255],  # Glue's limit for a column comment
                # Marked in the catalog too, where access rights would be set (Lake Formation on AWS).
                "Parameters": {"contains_pii": "true"} if meta.get("contains_pii") else {},
            }
        )

    return {
        "Name": model["name"],
        "Description": model.get("description", "")[:2048],
        "TableType": "EXTERNAL_TABLE",  # Glue only points to the files; dropping the table keeps them
        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE", "dbt_model": model["unique_id"]},
        "StorageDescriptor": {"Columns": columns, "Location": folder, **PARQUET_FORMAT},
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default="data/catalog/manifest.json")
    args = parser.parse_args()

    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    models = [
        node
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model" and node["fqn"][1] == LAYER  # fqn: [project, folder, ..., name]
    ]
    if not models:
        raise SystemExit(f"No {LAYER} model in {args.manifest}")

    glue = boto3.client("glue")  # endpoint, credentials and region from the environment, like init_floci.py

    try:
        glue.get_database(Name=DATABASE)
        print(f"Database already exists: {DATABASE}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={"Name": DATABASE, "Description": "Gold layer of sme-lake: star schema and marts (dbt)."}
        )
        print(f"Database created: {DATABASE}")

    for model in sorted(models, key=lambda m: m["name"]):
        table = table_input(model)
        try:
            glue.get_table(DatabaseName=DATABASE, Name=table["Name"])
            glue.update_table(DatabaseName=DATABASE, TableInput=table)
            action = "updated"
        except glue.exceptions.EntityNotFoundException:
            glue.create_table(DatabaseName=DATABASE, TableInput=table)
            action = "created"
        columns = table["StorageDescriptor"]["Columns"]
        pii = sum(1 for c in columns if c["Parameters"])
        print(f"Table {action}: {DATABASE}.{table['Name']} ({len(columns)} columns, {pii} personal) "
              f"-> {table['StorageDescriptor']['Location']}")

    # Check the catalog instead of trusting the calls above.
    registered = {
        table["Name"]: len(table["StorageDescriptor"]["Columns"])
        for page in glue.get_paginator("get_tables").paginate(DatabaseName=DATABASE)
        for table in page["TableList"]
    }
    expected = {model["name"]: len(model["columns"]) for model in models}
    wrong = {name: (count, registered.get(name)) for name, count in expected.items() if registered.get(name) != count}
    if wrong:
        raise SystemExit(f"Glue does not match dbt (model: expected columns, registered columns): {wrong}")
    print(f"OK: {len(expected)} {LAYER} tables in Glue, columns as in dbt")


if __name__ == "__main__":
    main()

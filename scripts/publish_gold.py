"""Publish the gold tables as Apache Iceberg tables in the Glue catalog (M12), once every dbt test has passed.

dbt builds gold inside the DuckDB warehouse and tests it there: that is the write and the audit, and nothing
reaches S3 before both are done. This script is the publish step. For each gold model of dbt's manifest:
1. read the table from the DuckDB warehouse (read-only);
2. create its Iceberg table if needed (database sme_lake_gold, files under <lake>/gold/<model>/);
3. keep the Iceberg schema in line with the dbt contract: added columns are added (union_by_name), a changed
   type is refused by Iceberg and stops the run; dbt's descriptions become column docs (Glue comments), the
   model description and the personal-data columns become table properties;
4. replace the content in one commit: a reader sees the previous version or the new one, never a mix;
5. housekeeping: expire the snapshots older than 7 days (time travel stays possible for a week), then delete
   the files that no snapshot references any more, only if older than a day (files of a commit in progress
   must stay). PyIceberg's expire_snapshots deletes no file by itself (measured in the spike, step 5).
   The single Parquet file that dbt wrote per gold table before M12 is such an unreferenced file: it goes too.

Schema changes and the data are two commits: during a few seconds a reader can see a new, empty column.
A column removed from a contract stays in the Iceberg table (empty in new rows) until dropped on purpose.

Run from the repository root, after a dbt build:
    uv run --env-file .env python scripts/publish_gold.py [--manifest dbt/target/manifest.json]
The default manifest is the one written by the Airflow task publish_catalog.
"""

import argparse
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import boto3
import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.table import Table

DATABASE = "sme_lake_gold"
LAYER = "gold"  # the folder of models/ whose models are published
SNAPSHOT_RETENTION = timedelta(days=7)
ORPHAN_MIN_AGE = timedelta(days=1)
WAREHOUSE_FILE = os.environ.get("DBT_DUCKDB_PATH", "data/warehouse.duckdb")  # same default as profiles.yml
LAKE_ROOT = os.environ.get("LAKE_ROOT", "s3://sme-lake")  # same default as the dbt macro lake_root()


def glue_catalog() -> Catalog:
    """PyIceberg's Glue catalog: Glue holds each table's pointer, S3 its files (both on Floci here)."""
    endpoint = os.environ["AWS_ENDPOINT_URL"]
    properties = {"type": "glue", "glue.endpoint": endpoint, "s3.endpoint": endpoint}
    for key, variable in [
        ("access-key-id", "AWS_ACCESS_KEY_ID"),
        ("secret-access-key", "AWS_SECRET_ACCESS_KEY"),
        ("region", "AWS_DEFAULT_REGION"),
    ]:
        properties[f"glue.{key}"] = properties[f"s3.{key}"] = os.environ[variable]
    return load_catalog("glue", **properties)


def documentation(model: dict) -> tuple[dict[str, str], dict[str, str]]:
    """dbt's documentation of a model: column docs, and the table properties."""
    docs, pii = {}, []
    for name, column in model["columns"].items():
        if column.get("description"):
            docs[name] = column["description"][:255]  # Glue's limit for a column comment
        meta = {**column.get("meta", {}), **column.get("config", {}).get("meta", {})}
        if meta.get("contains_pii"):
            pii.append(name)
    properties = {
        "Description": model.get("description", "").strip()[:2048],  # becomes the Glue table's description
        "pii_columns": ",".join(pii),  # Glue column parameters cannot be set through PyIceberg
        "dbt_model": model["unique_id"],
    }
    return docs, properties


def sync_schema_and_properties(table: Table, data: pa.Table, docs: dict, properties: dict) -> list[str]:
    """Bring the Iceberg schema and properties in line with dbt. Each step commits only if something changed."""
    changes = []
    before = {field.name for field in table.schema().fields}
    with table.update_schema() as update:
        update.union_by_name(data.schema)  # a changed type raises here: the run stops before any data is written
    if added := [name for name in data.schema.names if name not in before]:
        changes.append(f"columns added: {', '.join(added)}")
    stale = {name: doc for name, doc in docs.items() if table.schema().find_field(name).doc != doc}
    if stale:
        with table.update_schema() as update:
            for name, doc in stale.items():
                update.update_column(name, doc=doc)
        changes.append(f"{len(stale)} column docs updated")
    if changed := {key: value for key, value in properties.items() if table.properties.get(key) != value}:
        with table.transaction() as transaction:
            transaction.set_properties(changed)
        changes.append(f"properties set: {', '.join(changed)}")
    return changes


def referenced_files(table: Table) -> set[str]:
    """Every file a reader of a kept snapshot may open: current metadata, manifest lists, manifests, data."""
    files = {table.metadata_location}
    for snapshot in table.snapshots():
        files.add(snapshot.manifest_list)
        for manifest in snapshot.manifests(table.io):
            files.add(manifest.manifest_path)
            for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=True):
                files.add(entry.data_file.file_path)
    return files


def housekeeping(table: Table, s3) -> tuple[int, int]:
    """Expire old snapshots, then delete old unreferenced files. Returns (snapshots expired, files deleted)."""
    now = datetime.now(UTC)
    current = table.current_snapshot().snapshot_id
    cutoff_ms = int((now - SNAPSHOT_RETENTION).timestamp() * 1000)
    expired = [s.snapshot_id for s in table.snapshots() if s.timestamp_ms < cutoff_ms and s.snapshot_id != current]
    if expired:  # expiring nothing would still write a new metadata file
        table.maintenance.expire_snapshots().by_ids(expired).commit()

    keep = referenced_files(table)
    bucket, _, prefix = table.location().removeprefix("s3://").partition("/")
    orphans = [
        item["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/")
        for item in page.get("Contents", [])
        if f"s3://{bucket}/{item['Key']}" not in keep and item["LastModified"] < now - ORPHAN_MIN_AGE
    ]
    for key in orphans:
        s3.delete_object(Bucket=bucket, Key=key)
    return len(expired), len(orphans)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default="data/catalog/manifest.json")
    args = parser.parse_args()

    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    models = sorted(
        (node for node in manifest["nodes"].values()
         if node["resource_type"] == "model" and node["fqn"][1] == LAYER),  # fqn: [project, folder, ..., name]
        key=lambda node: node["name"],
    )
    if not models:
        raise SystemExit(f"No {LAYER} model in {args.manifest}")

    catalog = glue_catalog()
    s3 = boto3.client("s3")
    try:
        catalog.create_namespace(DATABASE, {"Description": "Gold layer of sme-lake: star schema and marts (dbt)."})
        print(f"Database created: {DATABASE}")
    except NamespaceAlreadyExistsError:
        print(f"Database already exists: {DATABASE}")

    warehouse = duckdb.connect(WAREHOUSE_FILE, read_only=True)
    warehouse.execute("SET TimeZone = 'UTC'")  # Iceberg stores timestamps with a time zone in UTC only

    problems = []
    for model in models:
        name = model["name"]
        location = f"{LAKE_ROOT}/{LAYER}/{name}"
        data = warehouse.execute(f'SELECT * FROM "{model["schema"]}"."{model["alias"] or name}"').to_arrow_table()
        docs, properties = documentation(model)

        changes = []
        try:
            table = catalog.load_table((DATABASE, name))
        except NoSuchTableError:
            table = catalog.create_table((DATABASE, name), schema=data.schema, location=location)
            changes.append("created")
        changes += sync_schema_and_properties(table, data, docs, properties)

        table.overwrite(data)  # one commit: the new version becomes visible at once
        expired, deleted = housekeeping(table, s3)

        published = int(table.current_snapshot().summary["total-records"])
        if published != data.num_rows:
            problems.append(f"{name}: {published} rows published, {data.num_rows} in the warehouse")
        print(f"{name}: {published} rows, snapshot {table.current_snapshot().snapshot_id}"
              f"{'; ' + '; '.join(changes) if changes else ''}; "
              f"{len(table.snapshots())} snapshots kept, {expired} expired, {deleted} old files deleted")

    if problems:
        raise SystemExit("; ".join(problems))
    print(f"OK: {len(models)} {LAYER} tables published as Iceberg in {DATABASE}")


if __name__ == "__main__":
    main()

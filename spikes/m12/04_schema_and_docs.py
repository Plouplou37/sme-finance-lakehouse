"""Iceberg spike (M12), step 4: dbt's contract becomes the Iceberg schema, and schema changes are controlled.

1. Publish dim_customer with what dbt documents (M7): the column descriptions as Iceberg column docs, the
   model description, and the personal-data columns as a table property. Show what Glue receives.
2. A compatible change, as when a contract gains a column: writing it without evolving the schema is
   refused; after union_by_name it is written, and Athena sees the new column.
3. An incompatible change, the M7 contract break (a timestamp becoming a date): Iceberg refuses to evolve
   the schema, so the table cannot silently change type under its readers.

Run from the repository root, after a dbt build (it reads dbt/target/manifest.json):
    uv run --env-file .env python spikes/m12/04_schema_and_docs.py
"""

import json
from pathlib import Path

import boto3
from common import NAMESPACE, WAREHOUSE, duckdb_on_s3, glue_catalog, gold_parquet, run_on_athena
from pyiceberg.exceptions import NoSuchTableError

MODEL = "dim_customer"
IDENTIFIER = f"{NAMESPACE}.{MODEL}"
MANIFEST = Path("dbt/target/manifest.json")


def dbt_documentation(model: str) -> tuple[str, dict[str, str], list[str]]:
    """The model's description, its column descriptions and its personal-data columns, from dbt."""
    nodes = json.loads(MANIFEST.read_text(encoding="utf-8"))["nodes"]
    node = next(n for n in nodes.values() if n["resource_type"] == "model" and n["name"] == model)
    docs, pii = {}, []
    for name, column in node["columns"].items():
        docs[name] = column.get("description", "")
        meta = {**column.get("meta", {}), **column.get("config", {}).get("meta", {})}
        if meta.get("contains_pii"):
            pii.append(name)
    return node.get("description", "").strip(), docs, pii


def main() -> None:
    duck = duckdb_on_s3()
    catalog = glue_catalog()
    glue = boto3.client("glue")
    problems = []

    # 1. Create the table from dbt's Parquet, documented from dbt's manifest.
    source = duck.execute(f"SELECT * FROM read_parquet('{gold_parquet(MODEL)}')").to_arrow_table()
    description, docs, pii = dbt_documentation(MODEL)
    try:
        catalog.drop_table(IDENTIFIER)  # start clean at each run of this step (the files stay, orphaned)
        print(f"previous {IDENTIFIER} dropped from Glue")
    except NoSuchTableError:
        pass
    table = catalog.create_table(
        IDENTIFIER,
        schema=source.schema,
        location=f"{WAREHOUSE}/{MODEL}",
        properties={"Description": description[:2048], "pii_columns": ",".join(pii)},
    )
    with table.update_schema() as update:
        for name, doc in docs.items():
            if doc:
                update.update_column(name, doc=doc[:255])  # 255: Glue's limit for a column comment
    table.overwrite(source)

    glue_table = glue.get_table(DatabaseName=NAMESPACE, Name=MODEL)["Table"]
    print(f"1. Glue Description: {glue_table.get('Description', '')[:70]}...")
    print(f"   Glue Parameters pii_columns: {glue_table['Parameters'].get('pii_columns')}")
    for column in glue_table["StorageDescriptor"]["Columns"][:6]:
        print(f"   {column['Name']:<14} {column['Type']:<10} {column.get('Comment', '')[:45]:<45} "
              f"{column.get('Parameters')}")
    commented = sum(1 for c in glue_table["StorageDescriptor"]["Columns"] if c.get("Comment"))
    print(f"   {commented} of {len(glue_table['StorageDescriptor']['Columns'])} columns have a comment in Glue")
    if commented != sum(1 for doc in docs.values() if doc):
        problems.append("some dbt descriptions did not reach Glue")

    # 2. A compatible change: one more column.
    duck.register("source", source)
    with_new_column = duck.execute(
        "SELECT *, CAST('standard' AS VARCHAR) AS customer_segment FROM source"
    ).to_arrow_table()
    before_change = table.current_snapshot().snapshot_id
    try:
        table.overwrite(with_new_column)
        problems.append("a new column was written without evolving the schema")
    except Exception as error:
        print(f"2. without schema change, refused: {type(error).__name__}: {str(error).splitlines()[0][:110]}")
    with table.update_schema() as update:
        update.union_by_name(with_new_column.schema)
    table.overwrite(with_new_column)
    rows = run_on_athena(f"SELECT customer_segment, count(*) FROM {MODEL} GROUP BY 1")
    print(f"   after union_by_name: written; Athena sees customer_segment = {rows}")
    old_columns = table.scan(snapshot_id=before_change).to_arrow().column_names
    print(f"   the snapshot before the change still reads {len(old_columns)} columns "
          f"(customer_segment {'present' if 'customer_segment' in old_columns else 'absent'})")

    # 3. An incompatible change: valid_from from a timestamp to a date.
    with_date_type = duck.execute(
        "SELECT * REPLACE (CAST(valid_from AS DATE) AS valid_from), "
        "CAST('standard' AS VARCHAR) AS customer_segment FROM source"
    ).to_arrow_table()
    try:
        with table.update_schema() as update:
            update.union_by_name(with_date_type.schema)
        problems.append("Iceberg accepted changing valid_from from a timestamp to a date")
    except Exception as error:
        print(f"3. timestamp -> date refused: {type(error).__name__}: {str(error).splitlines()[0][:110]}")
    current_type = catalog.load_table(IDENTIFIER).schema().find_field("valid_from").field_type
    print(f"   valid_from is still {current_type}")

    if problems:
        raise SystemExit("\n".join(problems))
    print("OK: dbt's documentation reaches Glue, a new column is an explicit schema change, "
          "and an incompatible type change is refused")


if __name__ == "__main__":
    main()

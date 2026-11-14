"""Iceberg spike (M12), step 2: an overwrite is invisible until its commit, and the old version stays readable.

Simulates the next daily run of gold: one open invoice of fct_invoices gets paid. The new version is written
inside a PyIceberg transaction, and the script looks at S3 and asks the readers twice:
1. inside the transaction, after the files are written but before the commit: the new files are on S3,
   yet every reader going through Glue still sees the old version;
2. after the commit: Glue points to a new metadata file, and every reader sees the new version.
Then it reads the old version again (time travel) through PyIceberg, DuckDB and Athena on Floci.

Run from the repository root, after 01_create_and_read.py (which resets the table to dbt's gold):
    uv run --env-file .env python spikes/m12/02_atomic_overwrite_and_time_travel.py
"""

from common import (
    NAMESPACE,
    as_text,
    duckdb_on_s3,
    glue_catalog,
    glue_pointer,
    list_files,
    run_on_athena,
)

MODEL = "fct_invoices"


def short(path: str | None) -> str:
    return path.rsplit("/", 1)[-1] if path else str(path)


def main() -> None:
    duck = duckdb_on_s3(iceberg=True)
    catalog = glue_catalog()
    table = catalog.load_table(f"{NAMESPACE}.{MODEL}")

    def state_through_glue(invoice_id: str) -> dict[str, tuple]:
        """What a reader sees right now: each one follows Glue's pointer."""
        pointer = glue_pointer(MODEL)["metadata_location"]
        duck_row = duck.execute(
            f"SELECT status, amount_outstanding FROM iceberg_scan('{pointer}') WHERE invoice_id = ?", [invoice_id]
        ).fetchone()
        athena_rows = run_on_athena(
            f"SELECT status, amount_outstanding FROM {MODEL} WHERE invoice_id = '{invoice_id}'"
        )
        return {
            "DuckDB": (duck_row[0], as_text(duck_row[1])),
            "Athena": tuple(athena_rows[0]) if athena_rows else None,
        }

    # Before: the current version, and one open invoice to pay.
    current = table.scan().to_arrow()
    duck.register("current_gold", current)
    invoice_id, status, outstanding = duck.execute(
        "SELECT invoice_id, status, amount_outstanding FROM current_gold "
        "WHERE status = 'open' ORDER BY invoice_id LIMIT 1"
    ).fetchone()
    old = (status, as_text(outstanding))
    old_snapshot = table.current_snapshot().snapshot_id
    old_metadata = table.metadata_location
    files_before = {key for key, _ in list_files(table.location())}
    print(f"before: snapshot {old_snapshot}, Glue points to {short(old_metadata)}")
    print(f"    {invoice_id}: status {old[0]}, outstanding {old[1]}")

    # The next run's result: the same table, with that invoice paid in full.
    new_version = duck.execute(
        """
        SELECT * REPLACE (
            CASE WHEN invoice_id = $id THEN 'paid' ELSE status END AS status,
            CASE WHEN invoice_id = $id THEN amount_incl_vat ELSE amount_paid END AS amount_paid,
            CASE WHEN invoice_id = $id THEN CAST(0 AS DECIMAL(18, 2)) ELSE amount_outstanding END
                AS amount_outstanding
        )
        FROM current_gold
        """,
        {"id": invoice_id},
    ).to_arrow_table()
    new = ("paid", "0.00")

    problems = []
    with table.transaction() as transaction:
        transaction.overwrite(new_version)
        # Files written, commit not done yet.
        added = sorted({key for key, _ in list_files(table.location())} - files_before)
        print(f"inside the transaction: {len(added)} new objects on S3, Glue still points to "
              f"{short(glue_pointer(MODEL)['metadata_location'])}")
        for key in added:
            print(f"    + {short(key)}")
        for reader, seen in state_through_glue(invoice_id).items():
            print(f"    {reader} sees {seen}")
            if seen != old:
                problems.append(f"{reader} saw the new version before the commit: {seen}")
    # Leaving the block commits: Glue's pointer moves to a new metadata file.

    pointer = glue_pointer(MODEL)
    table = catalog.load_table(f"{NAMESPACE}.{MODEL}")
    print(f"after the commit: Glue points to {short(pointer['metadata_location'])} "
          f"(previous: {short(pointer['previous_metadata_location'])})")
    for reader, seen in state_through_glue(invoice_id).items():
        print(f"    {reader} sees {seen}")
        if seen != new:
            problems.append(f"{reader} did not see the new version after the commit: {seen}")

    # Time travel: the old version is still there.
    print(f"time travel to snapshot {old_snapshot}:")
    old_rows = table.scan(row_filter=f"invoice_id == '{invoice_id}'", snapshot_id=old_snapshot).to_arrow()
    pyiceberg_old = (old_rows["status"][0].as_py(), as_text(old_rows["amount_outstanding"][0].as_py()))
    duck_old = duck.execute(
        f"SELECT status, amount_outstanding FROM iceberg_scan('{old_metadata}') WHERE invoice_id = ?", [invoice_id]
    ).fetchone()
    duck_old = (duck_old[0], as_text(duck_old[1]))
    for reader, seen in {"PyIceberg (snapshot id)": pyiceberg_old, "DuckDB (old metadata file)": duck_old}.items():
        print(f"    {reader} sees {seen}")
        if seen != old:
            problems.append(f"{reader} did not return the old version: {seen}")
    try:
        athena_old = run_on_athena(
            f"SELECT status, amount_outstanding FROM {MODEL} FOR VERSION AS OF {old_snapshot} "
            f"WHERE invoice_id = '{invoice_id}'"
        )
        verdict = "the old version" if athena_old and tuple(athena_old[0]) == old else "NOT the old version"
        print(f"    Athena (FOR VERSION AS OF) returned {athena_old}: {verdict}")
    except SystemExit as error:  # a gap to record, not a failure of Iceberg itself
        print(f"    Athena (FOR VERSION AS OF) not supported on Floci: {error}")

    print("snapshots kept:")
    for snapshot in table.snapshots():
        operation = snapshot.summary.operation.value if snapshot.summary else "?"
        print(f"    {snapshot.snapshot_id}  {operation:<9}  parent {snapshot.parent_snapshot_id}")

    if problems:
        raise SystemExit("\n".join(problems))
    print("OK: invisible until the commit, visible to every reader after it, and the old version still readable")


if __name__ == "__main__":
    main()

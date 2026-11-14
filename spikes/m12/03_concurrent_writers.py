"""Iceberg spike (M12), step 3: two writers at the same time. Does the second one get refused?

Iceberg's safety rests on the catalog: a commit must be refused if the table moved since the writer read it
(optimistic concurrency). Two levels are checked:
1. PyIceberg: writers A and B read the same version; each pays a different invoice; A commits first.
   B's commit must be refused, then B redoes its work on the new version: both invoices end up paid,
   and no update is lost.
2. Floci's Glue itself: PyIceberg checks the table before committing, but between that check and the
   UpdateTable call another writer can still commit. Only the catalog closes that window, by refusing an
   UpdateTable whose VersionId is out of date (on AWS: ConcurrentModificationException). The script sends
   one with a stale VersionId and the current content, so nothing changes if Floci wrongly accepts it.

Run from the repository root, after 01_create_and_read.py:
    uv run --env-file .env python spikes/m12/03_concurrent_writers.py
"""

import boto3
from botocore.exceptions import ClientError, ParamValidationError
from common import NAMESPACE, duckdb_on_s3, glue_catalog, run_on_athena
from pyiceberg.exceptions import CommitFailedException, ValidationException

MODEL = "fct_invoices"
IDENTIFIER = f"{NAMESPACE}.{MODEL}"
# The fields of GetTable's answer that UpdateTable accepts back in TableInput.
TABLE_INPUT_KEYS = (
    "Name", "Description", "Owner", "LastAccessTime", "LastAnalyzedTime", "Retention", "StorageDescriptor",
    "PartitionKeys", "ViewOriginalText", "ViewExpandedText", "TableType", "Parameters", "TargetTable",
)


def main() -> None:
    duck = duckdb_on_s3()
    catalog = glue_catalog()
    problems = []

    def pay(table_arrow, invoice_id: str):
        """The same table with one invoice paid in full: what one writer computes."""
        duck.register("base", table_arrow)
        return duck.execute(
            """
            SELECT * REPLACE (
                CASE WHEN invoice_id = $id THEN 'paid' ELSE status END AS status,
                CASE WHEN invoice_id = $id THEN amount_incl_vat ELSE amount_paid END AS amount_paid,
                CASE WHEN invoice_id = $id THEN CAST(0 AS DECIMAL(18, 2)) ELSE amount_outstanding END
                    AS amount_outstanding
            )
            FROM base
            """,
            {"id": invoice_id},
        ).to_arrow_table()

    # 1. Two writers read the same version.
    writer_a = catalog.load_table(IDENTIFIER)
    writer_b = catalog.load_table(IDENTIFIER)
    base = writer_a.scan().to_arrow()
    duck.register("base", base)
    invoice_a, invoice_b = [row[0] for row in duck.execute(
        "SELECT invoice_id FROM base WHERE status = 'open' ORDER BY invoice_id LIMIT 2"
    ).fetchall()]
    print(f"A and B both read snapshot {writer_a.current_snapshot().snapshot_id}; "
          f"A pays {invoice_a}, B pays {invoice_b}")
    result_a, result_b = pay(base, invoice_a), pay(base, invoice_b)

    writer_a.overwrite(result_a)
    print(f"A committed: snapshot {writer_a.current_snapshot().snapshot_id}")
    try:
        writer_b.overwrite(result_b)
        problems.append("B's commit was accepted on top of A's: A's payment may be lost")
        print("B committed too: NOT refused")
    except (CommitFailedException, ValidationException) as error:
        # PyIceberg first retries B's commit on top of A's version by itself, and refuses only if the two
        # changes conflict: B's overwrite would replace the files A just added (ValidationException).
        print(f"B refused ({type(error).__name__}): {error}")

    # B redoes its work on the version that now includes A's change.
    writer_b = catalog.load_table(IDENTIFIER)
    writer_b.overwrite(pay(writer_b.scan().to_arrow(), invoice_b))
    print(f"B retried on the new version and committed: snapshot {writer_b.current_snapshot().snapshot_id}")
    rows = run_on_athena(
        f"SELECT invoice_id, status FROM {MODEL} WHERE invoice_id IN ('{invoice_a}', '{invoice_b}') ORDER BY 1"
    )
    print(f"Athena now sees: {rows}")
    if [status for _, status in rows] != ["paid", "paid"]:
        problems.append(f"an update was lost: {rows}")

    # 2. The catalog's own check: an UpdateTable with an out-of-date VersionId.
    glue = boto3.client("glue")
    stale_version = glue.get_table(DatabaseName=NAMESPACE, Name=MODEL)["Table"].get("VersionId")
    with writer_b.transaction() as transaction:  # any commit moves the version: here, a table property
        transaction.set_properties(spike_step="03")
    current = glue.get_table(DatabaseName=NAMESPACE, Name=MODEL)["Table"]
    print(f"Glue VersionId: {stale_version} before a commit, {current.get('VersionId')} after")
    table_input = {key: current[key] for key in TABLE_INPUT_KEYS if key in current}
    try:
        glue.update_table(DatabaseName=NAMESPACE, TableInput=table_input, VersionId=stale_version)
        problems.append(f"Floci's Glue accepted an UpdateTable with the stale VersionId {stale_version}")
        print(f"Glue ACCEPTED an update with the stale VersionId {stale_version}")
    except ClientError as error:
        print(f"Glue refused the stale VersionId: {error.response['Error']['Code']}: "
              f"{error.response['Error'].get('Message')}")
    except ParamValidationError as error:  # boto3 refused to send it: the check did not happen
        problems.append(f"the stale-version request could not be built from GetTable's answer: {error}")

    if problems:
        raise SystemExit("\n".join(problems))
    print("OK: the second writer is refused, retries on the new version, and the catalog itself rejects "
          "an out-of-date commit")


if __name__ == "__main__":
    main()

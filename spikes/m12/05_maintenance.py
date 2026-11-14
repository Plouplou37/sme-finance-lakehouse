"""Iceberg spike (M12), step 5: housekeeping. What expiring snapshots removes, and what it leaves on S3.

Every overwrite of fct_invoices in steps 1 to 3 added a snapshot and about five files. This step:
1. counts the snapshots and the objects under the table's folder;
2. expires every snapshot but the current one with PyIceberg (expire_snapshots), then counts again:
   PyIceberg 0.12 removes the snapshots from the metadata only, it deletes no file (read in its code);
3. checks that an expired snapshot can no longer be read: time travel is not a backup;
4. computes which files the table still references (current metadata, manifest lists, manifests, data files)
   and deletes the others, as `remove_orphan_files` does in Spark;
5. reads the table again through Athena and PyIceberg: same rows, same total.

Run from the repository root, after steps 1 to 3:
    uv run --env-file .env python spikes/m12/05_maintenance.py
"""

from datetime import UTC, datetime

import boto3
import pyarrow.compute as pc
from common import NAMESPACE, as_text, glue_catalog, list_files, run_on_athena

MODEL = "fct_invoices"


def summary(files: list[tuple[str, int]]) -> str:
    data = [size for key, size in files if "/data/" in key]
    metadata = [size for key, size in files if "/metadata/" in key]
    return (f"{len(files)} objects: {len(data)} data files ({sum(data):,} B), "
            f"{len(metadata)} metadata files ({sum(metadata):,} B)")


def main() -> None:
    catalog = glue_catalog()
    table = catalog.load_table(f"{NAMESPACE}.{MODEL}")
    bucket = table.location().removeprefix("s3://").split("/", 1)[0]
    problems = []

    # 1. Before.
    snapshots = table.snapshots()
    oldest = snapshots[0].snapshot_id
    print(f"1. before: {len(snapshots)} snapshots; S3: {summary(list_files(table.location()))}")

    # 2. Expire every snapshot older than now; the current one is protected (head of branch main).
    table.maintenance.expire_snapshots().older_than(datetime.now(UTC)).commit()
    table = catalog.load_table(f"{NAMESPACE}.{MODEL}")
    files = list_files(table.location())
    print(f"2. after expire_snapshots: {len(table.snapshots())} snapshot(s); S3: {summary(files)}")

    # 3. An expired snapshot is gone for readers.
    try:
        table.scan(snapshot_id=oldest).to_arrow()
        problems.append(f"the expired snapshot {oldest} can still be read")
    except Exception as error:
        print(f"3. time travel to the expired snapshot {oldest}: {type(error).__name__}: {error}")

    # 4. What the table still references; everything else under its folder is an orphan.
    referenced = {table.metadata_location}
    for snapshot in table.snapshots():
        referenced.add(snapshot.manifest_list)
        for manifest in snapshot.manifests(table.io):
            referenced.add(manifest.manifest_path)
            for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=True):
                referenced.add(entry.data_file.file_path)
    orphans = [(key, size) for key, size in files if f"s3://{bucket}/{key}" not in referenced]
    print(f"4. referenced by the table: {len(referenced)} files; orphans: {len(orphans)} "
          f"({sum(size for _, size in orphans):,} B), deleted now")
    # In production: only orphans older than a few days, so that files of a commit in progress are kept.
    s3 = boto3.client("s3")
    for key, _ in orphans:
        s3.delete_object(Bucket=bucket, Key=key)
    print(f"   S3 now: {summary(list_files(table.location()))}")

    # 5. The table is intact.
    scanned = table.scan().to_arrow()
    expected = (str(scanned.num_rows), as_text(pc.sum(scanned["amount_incl_vat"]).as_py()))
    athena = tuple(run_on_athena(f"SELECT count(*), sum(amount_incl_vat) FROM {MODEL}")[0])
    print(f"5. PyIceberg {expected}, Athena {athena}")
    if athena != expected:
        problems.append(f"after the cleanup, Athena reads {athena} instead of {expected}")

    if problems:
        raise SystemExit("\n".join(problems))
    print("OK: old snapshots expired, their files removed by our own cleanup, and the table still reads the same")


if __name__ == "__main__":
    main()

"""Create the sme-lake bucket and its prefixes in Floci. Safe to run several times (idempotent).

Run from the repository root:
    uv run --env-file .env scripts/init_floci.py

boto3 reads the endpoint, the fake credentials, the region and the path-style setting
from environment variables (loaded from .env by uv). Nothing is hard-coded here.
"""

import boto3

BUCKET = "sme-lake"
PREFIXES = ["bronze/", "silver/", "gold/", "quarantine/", "checkpoints/", "bench/"]


def main() -> None:
    client = boto3.client("s3")

    # Create the bucket only if it is missing. Do not rely on create_bucket failing when the bucket
    # already exists: in us-east-1 (AWS and Floci), re-creating a bucket you own succeeds silently.
    bucket_names = [b["Name"] for b in client.list_buckets()["Buckets"]]
    if BUCKET in bucket_names:
        print(f"Bucket already exists: {BUCKET}")
    else:
        client.create_bucket(Bucket=BUCKET)
        print(f"Bucket created: {BUCKET}")

    # S3 has no real folders: a prefix is shown by an empty object whose key ends with "/".
    # Writing the same empty object again changes nothing, so this loop is idempotent.
    for prefix in PREFIXES:
        client.put_object(Bucket=BUCKET, Key=prefix)
        print(f"Prefix ready: {prefix}")

    # Check the result instead of trusting the calls above.
    # One listing per prefix: a listing returns at most 1000 keys, so once bronze/ holds many files,
    # a single listing of the whole bucket would never reach gold/ or silver/.
    missing = []
    for prefix in PREFIXES:
        response = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
        if not response.get("Contents"):  # "Contents" is absent when nothing matches
            missing.append(prefix)
    if missing:
        raise SystemExit(f"Missing prefixes in s3://{BUCKET}: {missing}")  # exit code 1: make up stops
    print(f"OK: s3://{BUCKET} has its {len(PREFIXES)} prefixes")


if __name__ == "__main__":
    main()

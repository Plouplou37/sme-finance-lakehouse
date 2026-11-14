# Versions

Every version here was tested together in M0 (see `docs/adr/0001-floci.md`).

Rules:
- **Never `latest`.** Images are pinned by tag **and** digest: the tag is for humans, the digest guarantees the exact same image even if the tag is re-published.
- **At least 14 days old** when chosen, unless a test or a known bug justifies an exception (noted below).
- **Downloaded jars are checked** against the SHA-1 published by Maven Central.
- Upgrades are deliberate: change one line, re-run the matching M0 test, update this file.

Checked on 2026-11-07.

## Containers

| Component | Version (tag@digest) | Why this version |
|---|---|---|
| Floci (AWS emulator) | `floci/floci:2.4.0@sha256:5ee85db604c4aa74ac76e07ea5cc83086f37bb472bf3dbb97e34019e48efae9c` | Latest release; fixes bugs of 2.3.0. Chosen although under 14 days old, and validated by the M0 tests. Fallback: 2.3.0 |
| floci-duck (Athena engine) | `floci/floci-duck:0.3.0@sha256:7d9a084d83ddace3bd760db574ee1bf25012a90503099c83e97b691d738e2360` | Latest release when Floci 2.4.0 shipped (2026-10-31); 0.4.x was under 14 days old and only adds BigQuery helpers |
| Redpanda (Kafka API) | `redpandadata/redpanda:v26.2.6@sha256:8b1076e0d61bfceb66594dc01536427ce0b529f8ead35fdab9763ab6085891ce` | Replaces Floci's MSK, whose broker disappears when Floci restarts |
| AWS CLI (test client) | `amazon/aws-cli:2.38.5@sha256:c8156a0e74e13929a42d7e60f0d4e14e7d28d980cd21822505d4bff80e5460c1` | Latest release at least 14 days old |
| Python base (DuckDB image) | `python:3.12.15-slim@sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704` | Python 3.12 line |
| Spark | `apache/spark:4.2.0-scala2.13-java17-python3-ubuntu@sha256:84a4eedb1abcf36a90808d5a1310e3e910b78c85d85aaa1599e31af5f862ed59` | Newest Spark release at least 14 days old (2026-07-14); Java 17 is Spark's default build target. Fallback: 4.1.3. Also runs the Spark Connect server (M8) |
| Airflow (base of our image) | `apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1` | Newest Airflow release at least 14 days old (2026-09-17; 3.4.0 was 5 days old on 2026-11-11). Docker Scout reports known vulnerabilities in this base image (18 critical, 118 high on 2026-11-11), not triaged |
| PostgreSQL (Airflow's metadata database) | `postgres:16.14@sha256:95206741a5b214807675e14165369d05b93a9cf692223b616d07cca227e74b0b` | 16.15 was 7 days old (2026-11-07), so 16.14 (2026-08-05). Minor versions share the same storage format: the existing volume was kept |

## Libraries

| Library | Version | Why this version |
|---|---|---|
| DuckDB (Python) | `duckdb==1.5.6` | Last 1.5 release; dbt-duckdb only tests DuckDB 2.0 as a pre-release |
| Hadoop (inside Spark) | 3.5.0 | Bundled by Spark 4.2.0: not chosen, imposed |
| hadoop-aws (s3a connector) | `org.apache.hadoop:hadoop-aws:3.5.0` — SHA-1 `9e594525d264c0db653c7f68da98b245f7d61ea5` | Must equal the Hadoop version exactly |
| AWS SDK v2 | `software.amazon.awssdk:bundle:2.35.4` — SHA-1 `7252265e3970b214708e68a8b74a8fa8c875af1e` | Required by hadoop-aws 3.5.0; includes the fix for third-party S3 stores (HADOOP-19490 / HADOOP-19654) |
| S3 analytics accelerator | `software.amazon.s3.analyticsaccelerator:analyticsaccelerator-s3:1.3.1` — SHA-1 `6c9bd0f6c440c9a78e82d272f5f0252d942419f6` | Compile dependency of hadoop-aws 3.5.0. Its stream is disabled (`fs.s3a.input.stream.type=classic`) because of an open memory leak (HADOOP-20058) |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0` — SHA-1 `98f199d5da59b1e152dae941cc5037a6495464ad` | Must equal the Spark version (M4) |
| Spark Kafka token provider | `org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.2.0` — SHA-1 `807ceb26e2fa509259e74a202d5f2d790446f515` | Dependency of the connector, same version |
| Kafka client (inside Spark) | `org.apache.kafka:kafka-clients:3.9.2` — SHA-1 `a06edbaa01458ed2e8cf1be69a18c2f231fda6e0` | Version declared by the connector's POM: not chosen, imposed |
| Apache Commons Pool | `org.apache.commons:commons-pool2:2.13.1` — SHA-1 `cf7e8c0242d4c483c42c083b8188ab360ab60f9a` | Version declared by the connector's POM |

## Host tools

Installed on the machine; everything else runs in containers. Tested in M1 (2026-11-08).

| Tool | Tested version | Note |
|---|---|---|
| Docker Desktop | Docker 28.3.0, Compose 2.38.1 | Compose v2 is required (`docker compose`, `up --wait`) |
| GNU Make | 3.82.90 | Recipes are single commands, so they run in cmd.exe (Windows) and in sh (Linux) |
| uv | 0.8.11 | Upgrade to 0.13.5 postponed: the GitHub API was down when we tried |
| Git | 2.46.0 | Line endings are forced to LF by `.gitattributes` |
| Python | 3.12 (set in `.python-version`) | Same line as the container images (3.12.15). uv uses an installed 3.12 or downloads one |

## Python packages (host)

| Package | Version | Why this version |
|---|---|---|
| boto3 | `1.43.126` (botocore `1.43.126`) | Latest release when added in M1, not checked against the 14-day rule (boto3 ships almost daily). All packages, direct and indirect, are locked with their hashes in `uv.lock` |
| confluent-kafka | `2.16.0` | Kafka client (M3), a wrapper around librdkafka. Newest release at least 14 days old (2026-10-07; 2.16.1 was 12 days old). Ships a Windows wheel for CPython 3.12 |
| pytest (dev only) | `9.1.1` | Tests (M2). Newest release at least 14 days old (2026-06-19; 9.2.0 was 12 days old). Its 5 dependencies were not checked against the 14-day rule |
| dbt-core | `1.12.6` | Transformations (M5). Newest release at least 14 days old (2026-10-14). PyPI now recommends installing `dbt`, and says the `dbt-core` name will be deprecated in the long term |
| dbt-duckdb | `1.11.1` | dbt adapter for DuckDB (M5), released 2026-10-26 (16 days old when added) |
| duckdb (host) | `1.5.6` | Engine of dbt (M5): the version tested in M0. 2.0.0 is a major release, and its compatibility with dbt-duckdb is not documented |

dbt-core brings about 50 indirect dependencies (56 packages installed in M5): they are locked with their hashes in `uv.lock`, but were not checked one by one against the 14-day rule.

## Python packages (Airflow image, M8)

Installed by `airflow/Dockerfile`, each tool in its own virtual environment, so that none of them can break Airflow's own dependencies.

| Package | Version | Why this version |
|---|---|---|
| dbt-core, dbt-duckdb, duckdb | `1.12.6`, `1.11.1`, `1.5.6` | The same versions as on the host, so a model behaves the same in Airflow and by hand |
| pyspark-client | `4.2.0` | Thin Spark Connect client (no Java): must match the Spark server. Released 2026-07-14; 4.2.1 was under 14 days old |
| pandas | `<3` | Needed by the Spark client, which warns that it does not fully support pandas 3 yet (seen in the M8 spike) |
| boto3 (in dbt's environment) | `1.43.126` | M9: the Glue and Athena scripts run there, next to DuckDB. The version locked on the host |

## Not pinned

| Component | Status |
|---|---|
| DuckDB `httpfs` extension | Downloaded by floci-duck at first start and kept in the `floci-duck-ext` volume, and by DuckDB on the host at the first dbt run. Signed by DuckDB, but it can receive updates: installed versions not recorded yet |
| Dependencies of the Airflow image's packages | `pip install` resolves them when the image is built, with no lock file: a rebuild can bring newer versions. A constraints file (or `uv.lock`) would fix them |
| pandas in the Airflow image | A range (`<3`), not an exact version |

# ADR 0001: Use Floci as the local AWS emulator

- **Status:** Accepted (end of M0)
- **Date:** 2026-11-07

## Context

The project builds an AWS-style data platform (S3 data lake, Glue catalog, Athena queries, Kafka ingestion) for learning and demonstration. It must run on one laptop with these constraints:

- **No cost and no real cloud account:** nothing may be billed, and no real credentials may exist.
- **Public repository:** anyone must be able to clone it and run it with Docker only.
- **Limited memory:** Docker runs in a WSL2 VM capped at 8 GB, shared by every service.
- **Honesty:** we must not claim that an emulated service behaves like AWS without testing it.

So we need a local emulator of the AWS services we use, and we need to know exactly where it differs from AWS before building on it. M0 tested each risky point from a separate container, as a client would.

## Feasibility results (M0)

Floci version under test: `floci/floci:2.4.0@sha256:5ee85db604c4aa74ac76e07ea5cc83086f37bb472bf3dbb97e34019e48efae9c`.
All tests run from a **separate container** on the same Docker network (spike: `spikes/m0/docker-compose.yml`).

| Test | Result | Evidence | Decision |
|---|---|---|---|
| S3 from another container (AWS CLI 2.38.5) | ✅ Pass | `awscli` container: bucket `sme-lake` created, 18-byte file written then read back identical; no checksum error with the CLI's default CRC checksums | Keep Floci for S3 |
| Persistence after restart | ✅ Pass with `persistent` mode | Clean restart: data kept. `docker kill` right after a write: object **lost** in `hybrid` mode, **kept** in `persistent` mode. `down` then `up` (volume kept): data kept | Use `FLOCI_STORAGE_MODE=persistent`; never use `hybrid` |
| Docker socket mount | ✅ Pass | `/var/run/docker.sock` mounted into `floci` (checked with `docker inspect`); Floci started a sibling Redpanda container through it, without `FLOCI_RUN_AS_ROOT` | **Socket removed** once floci-duck runs as our own service (see the Glue + Athena row): Floci then starts no containers. Without it, Floci logs `Docker is not reachable` and lists the services that would need it; S3, Glue and Athena are not among them. Socket access is root-equivalent, so not mounting it is safer |
| DuckDB httpfs from a container (DuckDB 1.5.6, Python 3.12.15-slim) | ✅ Pass | `duckdb` container wrote `amounts.parquet` to `s3://sme-lake/m0/duckdb/` with `COPY ... TO`, then read it back: 1,000 rows and a total of 749,250.00, both checked by assertions. Settings: `URL_STYLE 'path'`, `USE_SSL false`, endpoint `floci:4566`. **Not yet proven:** dbt-duckdb external materializations and partitioned writes (`PARTITION_BY`) | Keep DuckDB 1.5.6 on Python 3.12.15-slim; re-check partitioned writes in M5 |
| Kafka (MSK) from a container | ⚠️ Works while running, **fails after a Floci restart** | `create-cluster` → `ACTIVE`; `rpk` in another container produced and consumed (partition 0, offset 0). After `docker compose restart floci`: the broker container no longer existed (`docker ps -a` empty) while `describe-cluster` still returned `ACTIVE` and consumers got `no route to host`. Bootstrap address is a dynamic IP (`172.18.0.4:9092`), not the container name the docs announce | **Do not use MSK.** Fallback (planned): Redpanda as a Compose service |
| Fallback: Redpanda v26.2.6 as a Compose service | ✅ Pass | 3 messages (offsets 0–2) read back identical after `restart` and after `down`/`up`; stable address `redpanda:9092`; 82 MiB used, capped at 1 GiB | **Use a single Redpanda broker** (1 GiB cap, pinned by digest). Lost: only the MSK control-plane API. `dev-container` mode (no fsync, topic auto-creation) to revisit in M3 |
| Glue + Athena (floci-duck 0.3.0) | ✅ Pass for our use, with limits | Glue table `m0.amounts` (`EXTERNAL_TABLE`, Parquet SerDe, location `s3://sme-lake/m0/duckdb/`) created from a JSON file and read back with `get-table`. Athena `SELECT count(*), sum(amount)` returned 1000 and 749,250.00, the same values as the DuckDB test. Floci started the sidecar with the pinned image (`7d9a084d83dd`). Copying the file into the prefix doubled the result (2,000 rows) with no change in Glue: a Hive-style table is everything under its prefix. Syntax probes: `WITH` runs; DuckDB-only `SELECT * EXCLUDE (id)` runs; `FROM … SELECT` returns `SUCCEEDED` without running (see gaps). Invalid query `SELECT unknown_col FROM amounts` → `FAILED`, with DuckDB's `Binder Error` as `StateChangeReason`. **floci-duck as a Compose service:** with `FLOCI_SERVICES_DUCK_URL=http://floci-duck:3000`, the same query returned 1000 and 749,250.00, no `floci-aws-duck` container appeared, memory 15 MiB of a 512 MiB cap, and the result stayed the same after removing the Docker socket. **Not yet tested:** Iceberg tables | **Keep Glue + Athena for M9**, with floci-duck as our own Compose service (pinned, 512 MiB cap, httpfs kept in a volume, no Docker socket). Rules: write SQL common to Trino and DuckDB; the Athena client treats `SUCCEEDED` without a result file or columns as an error; direct DuckDB queries stay the reference. Iceberg stays an optional bonus, not needed for M9 |
| Spark s3a from a container (Spark 4.2.0, Java 17, hadoop-aws 3.5.0, AWS SDK bundle 2.35.4) | ✅ Pass (local mode, small volume) | `spark-submit` in local mode (`local[2]`: one JVM, logs show `executor ID driver`, `Running Spark version 4.2.0`, `Java version 17.0.19`). Jars baked into the image and checked against Maven Central SHA-1 values at build time. Read DuckDB's `s3a://sme-lake/m0/duckdb/` with the schema kept (`id long`, `amount decimal(12,2)`): 1,000 rows and 749,250.00. Wrote `s3a://sme-lake/m0/spark/amounts/` with `mode("overwrite")`: one file `part-00000-…-c000.snappy.parquet` (9,724 bytes) plus `_SUCCESS`; read back: same two values. No checksum error with Floci. **Not yet proven:** Kafka source (`spark-sql-kafka` not installed yet), streaming checkpoints on s3a, S3A committers and concurrent writes, larger volumes and memory use | **Keep Spark 4.2.0 (Java 17) + hadoop-aws 3.5.0 + AWS SDK bundle 2.35.4**, baked into the image and checked by SHA-1, with the s3a settings in `spark-defaults.conf` (path-style, plain HTTP, region, `input.stream.type=classic` because of HADOOP-20058). Fallback: Spark 4.1.3 (Hadoop 3.4.2) |
| dbt-athena against Floci | ⏭️ Not run (decision) | Not tested. Predicted to fail: dbt-athena builds tables with Athena CTAS (`CREATE TABLE … WITH (format = …) AS SELECT`), and the Glue + Athena test showed that Floci sends non-`SELECT`/`WITH` statements to DuckDB unchanged, where Athena-only syntax is rejected (`UNLOAD` → `Parser Error`) | **Use dbt-duckdb** (already the default; it was the planned fallback). Revisit only if a real AWS account is ever used |

### Observed gaps vs. real AWS

- **Durability:** in `hybrid` mode Floci acknowledges a write (HTTP 200) before it reaches disk, so a crash loses acknowledged objects. Real S3 only acknowledges once the object is durably stored.
- **CreateBucket idempotence:** re-creating an existing bucket succeeds in `us-east-1` (same quirk as real AWS in that region). Our init script must check existence explicitly instead of relying on this.
- **Docker socket:** without `/var/run/docker.sock`, MSK runs no containers, and Athena fails unless `FLOCI_SERVICES_DUCK_URL` points to a running floci-duck (Floci 2.4.0 logs the full list of affected services at startup).
- **MSK state vs. reality:** after a Floci restart, cluster metadata is reloaded (`msk-clusters.json`) and reported `ACTIVE`, but the broker container is gone: a "zombie" cluster, with no error surfaced to clients until they connect.
- **MSK bootstrap address:** a dynamic container IP instead of the documented `<container-name>:9092`; it must be re-fetched with `get-bootstrap-brokers`, never hard-coded.
- **Containers spawned by Floci** have no memory limit, and their volumes (`floci-aws-msk-*`) are not part of the Compose project, so `docker compose down -v` does not remove them.
- **Athena engine:** Floci runs Athena queries in DuckDB (`floci-duck` sidecar), not Trino like real Athena engine v3, and does **not** check Athena syntax: the DuckDB-only query `SELECT * EXCLUDE (id) FROM amounts LIMIT 1` returned `SUCCEEDED` with the single column `amount`. SQL that passes locally may fail on AWS, and comparing "Athena vs DuckDB" results on Floci compares DuckDB with DuckDB.
- **Athena silent success:** the DuckDB-only query `FROM amounts SELECT count(*) AS n_rows` returned `SUCCEEDED` in 77 ms with `Rows: []`, `ColumnInfo: []`, and **no result file** in the output location: it was never executed. A `count(*)` always returns one row, and real Athena (Trino) would reject this syntax. A standard CTE (`WITH t AS (...) SELECT count(*) ...`) runs normally and returns 1000. Likely cause (consistent with all probes, not confirmed in Floci's code): Floci wraps only statements starting with `SELECT` or `WITH` in `COPY (…) TO 's3://…'`; any other statement is sent to DuckDB as is, so it runs but its rows are discarded. Evidence: `UNLOAD (SELECT * FROM amounts) TO … WITH (format = 'PARQUET')` → `FAILED` with DuckDB `Parser Error: syntax error at or near "UNLOAD"`, and the error shows the statement **not** wrapped in `COPY`. Consequence: our Athena client must treat `SUCCEEDED` with no result file or no columns as an error.
- **Athena `UNLOAD` not supported:** real Athena exports results as Parquet, ORC, Avro or JSON with `UNLOAD`; Floci passes it to DuckDB, which rejects it (`Parser Error`). CTAS (`CREATE TABLE … WITH (format = …) AS SELECT`) not tested.
- **Athena error messages:** a real error does return `FAILED`, but `StateChangeReason` carries DuckDB's message (`floci-duck execute returned HTTP 500: … Binder Error: Referenced column "unknown_col" not found …`), not Athena's (`COLUMN_NOT_FOUND …`). It also shows that Floci wraps the query in `COPY (…) TO 's3://…'`. Clients must branch on `State`, never parse the message text.
- **Athena result types:** `GetQueryResults` reports every column as `varchar` in `ColumnInfo` (real Athena reports `bigint`, `decimal(p,s)`, etc.). Values are strings in both cases, but clients that rely on column metadata (JDBC, BI tools) would see text.
- **Athena sidecar started by Floci:** `floci-aws-duck` runs with no memory limit (11 MiB idle, limit shown as the whole VM, 7.76 GiB), no volume (httpfs downloaded again for each new container), and on the default `bridge` network as well as ours. **Resolved** by running floci-duck as our own Compose service, the same pattern as the Redpanda fallback.
- **Observed once, not reproduced:** shortly after Floci was recreated, `aws s3 ls s3://sme-lake/m0/duckdb/` returned an empty listing with no error, while `--recursive` showed the object a minute later. Repeating the sequence (`up --force-recreate`, then two immediate listings) returned the object both times. Cause unknown. Consequence for M1: scripts must treat an unexpected empty listing as an error, not as "nothing to do".

## Decision

We use **Floci 2.4.0 to emulate S3, Glue and Athena**. It imitates the AWS APIs well enough for our use: the same calls, made with the same clients (AWS CLI, SDK, `s3a`), so the project's code is the code we would run on AWS.

Behind the API, the services do not work like AWS. Athena runs DuckDB, not Trino. MSK starts a Redpanda container that does not survive a restart. Some outputs differ too (column types, error messages). Nothing runs on AWS infrastructure either, so speed, latency and cost results do not transfer to AWS.

Therefore:
- Kafka runs on our own Redpanda service.
- Athena's engine (floci-duck) is our own service.
- Floci gets no Docker socket and stores data in `persistent` mode.
- Every gap listed above must be re-checked before any real AWS deployment.

## Alternatives considered

| Option | Why not chosen |
|---|---|
| **Real AWS account** | Costs money and needs real credentials, both forbidden here; a public repo could not be run by others without their own account. |
| **MinIO** | S3-compatible storage only: no Glue catalog, no Athena, no Kafka API. It would have been the fallback for S3 if Floci had failed the S3 or s3a tests. |
| **LocalStack** | The best-known AWS emulator. Not evaluated in this spike: Floci passed the tests we needed, so we did not compare further. |
| **Plain local services, no AWS emulation** (DuckDB on local files, Redpanda) | Would work, but the project would no longer show the AWS concepts (S3 paths, Glue catalog, Athena) that the target roles use. Kept as the fallback for queries (direct DuckDB). |

## Consequences

**What the project can show:** the same client code and configuration as on AWS (S3 paths with `s3a://` and `s3://`, Glue tables, Athena queries), versions pinned and tested together, and a stack that starts with Docker only.

**What it does NOT prove about a real AWS deployment:**
- IAM permissions, networking (VPC, endpoints), encryption and security settings: Floci accepts any credentials.
- Performance, latency, quotas and costs (for example Athena's price per data scanned).
- SQL compatibility with Athena: Floci runs DuckDB, not Trino (see the gaps above).
- Managed Kafka (MSK) behaviour: we run Redpanda instead.
- S3 durability guarantees: they depend on Floci's storage mode, not on S3.

**Rules that follow from the tests:**
- `FLOCI_STORAGE_MODE=persistent`, never `hybrid`.
- No Docker socket: every container is declared in Compose (Redpanda, floci-duck), pinned and memory-capped.
- Scripts check what exists instead of assuming it; an unexpected empty listing is an error.
- Athena clients branch on `State` and treat `SUCCEEDED` without a result file or columns as an error; SQL stays common to Trino and DuckDB.
- Every image is pinned by tag and digest; every downloaded jar is checked against its published SHA-1.

# Project journal

## M0: prerequisites and Floci feasibility (2026-11-07)

**Goal:** check the machine and remove the unknowns before building anything.

**Machine**
- Laptop with 12 GB of RAM: below the 16 GB recommended. Decision: keep going in a memory-saving mode (Compose profiles, a memory cap on every container) with the WSL2 VM set to 8 GB.
- Incident: Docker Desktop reported "Virtualization support not detected". Cause: hardware virtualization (VT-x) was disabled in the BIOS. Found with `systeminfo` ("Virtualization Enabled In Firmware: No"), fixed in the BIOS. Lesson: check the lowest layer first.

**Plan change**
- Ingestion (Kafka, Spark) is treated as a core skill, as much as modelling (dbt): Kafka and Spark modules are deepened, and the Spark vs DuckDB benchmark becomes mandatory.

**Tests (details in `docs/adr/0001-floci.md`)**

| Test | Result | Decision |
|---|---|---|
| S3 from another container | ✅ | Keep Floci for S3 |
| Persistence | ✅ with `persistent` mode | `hybrid` lost an acknowledged object after `docker kill`: never use it |
| DuckDB httpfs | ✅ | DuckDB 1.5.6 |
| Docker socket | ✅, then removed | Not needed once every container is declared in Compose |
| Kafka via Floci's MSK | ⚠️ | Broker disappears when Floci restarts: use Redpanda as a Compose service |
| Glue + Athena | ✅ with limits | Keep for M9; Athena runs DuckDB, not Trino; floci-duck declared in Compose |
| Spark s3a | ✅ | Spark 4.2.0 + hadoop-aws 3.5.0 + AWS SDK 2.35.4 |
| dbt-athena | not run | Predicted to fail (CTAS); dbt-duckdb is the default |

**Main lessons**
- Test from a separate container, as a real client would: addresses, networks and credentials are part of what is tested.
- Watch for silent successes: an empty listing, a `SUCCEEDED` query with no result, an `assert` that can never fail.
- Write a prediction before each experiment; two hypotheses were refuted by the Athena probes, which is how the real behaviour was found.
- A Hive-style table is everything under its prefix: copying one file doubled a total, which is why re-runs must replace data, not add to it.
- Fewer privileges: declaring floci-duck ourselves removed the need to give Floci the Docker socket (root-equivalent).

**Measured memory (idle):** Floci ~85–120 MiB, Redpanda ~320 MiB, floci-duck ~15 MiB, Spark job within a 1.5 GiB cap.

**Still unproven after M0 (to test in their modules):** Spark reading Kafka and streaming checkpoints on s3a (M4); dbt-duckdb external materializations and partitioned writes (M5); Airflow, including how it starts task containers without a Docker socket (M8); memory use with the whole stack running at once; Redpanda's `dev-container` mode (no fsync) to revisit in M3.

**Status:** M0 closed on 2026-11-07.

**Next:** M1, repository foundations (Compose for the real stack, `.env`, idempotent init script, Makefile, uv with a lock file, public repository).

## M1: repository foundations (2026-11-07 to 2026-11-08)

**Goal:** an environment that starts with one command, in a clean public repository.

**What was built**
- Git setup: `.gitignore` (`.env`, `.venv/`, data files) and `.gitattributes` (LF line endings for every file). Commits use GitHub's noreply address.
- uv project: Python 3.12, `pyproject.toml` and `uv.lock` (exact versions with hashes). boto3 is the only dependency so far.
- Root `docker-compose.yml` (project `sme-lake`): Floci, floci-duck and Redpanda, with pinned images (tag and digest) and memory caps. AWS CLI and rpk sit in a `tools` profile. Fake credentials come from `.env`, a copy of `.env.example`; Compose stops with a clear message if it is missing.
- `scripts/init_floci.py`: creates the `sme-lake` bucket and its 6 prefixes. Idempotent, and it ends with a check that exits with a non-zero code.
- `Makefile`: `up` (start, wait until healthy, init, status), `down`, `reset` (also deletes the volumes).
- README (architecture, getting started, progress) and host tools in `VERSIONS.md`.

**Success criteria**

| Criterion | Result |
|---|---|
| `make reset` then `make up` gives a healthy environment | ✅ Volumes recreated, `Bucket created`, 6 prefixes checked |
| `make up` twice does not fail | ✅ Second run: `Bucket already exists`, existing data kept |
| No secret in Git | ✅ `.env` is ignored; `.env.example` holds fake values only |
| Public repository with a README | ✅ Pushed to GitHub |
| Works on another machine | ✅ `make up` succeeded on the first try on a second Windows PC |

**Decisions**
- Python runs on the host (through uv) and in containers, on the 3.12 line.
- The init script uses Python and boto3 rather than AWS CLI commands: real error handling, and boto3 is reused in M9. Cost: uv becomes a host prerequisite.
- `make` runs on the host, not in a container: a container would need the Docker socket to start the others.
- Terraform or OpenTofu: a possible bonus, run in a container, after a small spike to check it works against Floci.
- From M2: one branch per module, merged through a pull request.

**Main lessons**
- Check before acting: an old `make` (3.82.90, 2012) was hiding the one just installed, and the Git config held a personal email that every public commit would have shown. Both were caught by looking at what already existed.
- Never rely on an error to detect a duplicate: in us-east-1, creating a bucket you already own succeeds silently. The script checks the bucket list first.
- Silent successes again: a test on the keys of the boto3 response was always true. Print the real response before writing a test on it.
- Idempotent means the same final state, not "do nothing": re-running the init rewrites the empty prefix markers, and a test file under `bronze/` survived, because S3 prefixes are not folders.
- Containers read the working tree, not Git: `.gitattributes` alone does not protect a script mounted into a container, so the editor also writes LF.

**Still open:** floci-duck takes about 40 s to stop (seen in M0; it probably ignores SIGTERM, options to check: `init: true`, `stop_grace_period`); the httpfs version is not recorded yet (M9); the uv upgrade to 0.13.5 is postponed.

**Status:** M1 closed on 2026-11-08.

**Next:** M2, data generator.

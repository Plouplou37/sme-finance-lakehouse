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

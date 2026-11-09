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

## M2: data generator (2026-11-08)

**Goal:** a realistic and reproducible flow of fictional accounting events, with anomalies known in advance.

**What was built**
- Event contract (`docs/event-contract.md`): 3 topics, 7 event types and a common envelope. Amounts are decimal strings, update events carry the full state, and schema version 2 adds `payment_method` to `payment_received`.
- Generator (`generator/`): `uv run python -m generator` with `--seed`, `--days`, `--companies`, `--start-date`, `--out` and one rate option per anomaly. It writes one JSON Lines file per topic, in arrival order, and `manifest.json`.
- Simulation: customers with a payer profile (good, average, bad); an invoice state machine (issued, sometimes updated, then cancelled, unpaid, or paid on time or late, in 1 to 3 payments, rarely refunded); address changes, which will feed the SCD2 of M6.
- Anomalies: duplicates, late arrivals, zero or negative amounts, unknown currencies, unknown references, overpayments, and the switch to schema version 2 in the middle of the period.
- 13 pytest tests.

**Success criteria**

| Criterion | Result |
|---|---|
| Two runs with the same `--seed` give the same result | ✅ Identical files, byte for byte (test) |
| The manifest matches the written events | ✅ Recounted from the files by topic, type and schema version (test) |
| `--days`, `--companies` and anomaly rates are configurable | ✅ Tested, including all rates at 0 |
| Each anomaly type has a pytest test | ✅ 7 anomaly tests, plus the contract rules on valid invoices: 13 passed in 0.89 s |

**Measured:** default run (5 companies, 90 days): 2,578 events, 146 anomalies. 50 companies over 365 days: 116,140 events in 6.7 s.

**Decisions**
- Amounts are decimal strings, never floats: in binary, `0.1 + 0.2` is `0.30000000000000004`, and a reconciliation to the cent would fail.
- `event_id` is a UUID created once at the source: it is the deduplication key. `invoice_id` links the events of an invoice and is its Kafka key.
- Business dates (`issue_date`, `payment_date`) are kept apart from `event_time`: they decide the revenue month and the payment delay.
- Update events carry the full state: each one is a ready-made version for the SCD2, even when events arrive out of order.
- Only EUR is valid, to avoid exchange rates.
- The generator writes files, not Kafka: it can be tested without any service, and M3 publishes the files.
- Each customer has a payer profile, so that "which customers pay late" has a real answer to find.
- One anomaly touches exactly one event (a broken invoice is a new invoice with no payment), so the manifest stays an exact reference for M5 and M7.

**Main lessons**
- The same seed is not enough: same seed, same code and same versions give the same result. Adding the invoices changed the number of customers from 154 to 147 with `--seed 42`.
- `uuid4()` and `datetime.now()` break reproducibility: one random generator built from the seed, and a fixed start date.
- The end of the period is a snapshot: payments due later are not written yet (859 payments for 1,359 invoices over 90 days). Unpaid does not mean late.
- A test that loops over an empty list always passes: each anomaly test first checks that the anomaly was injected.

**Still open:** the producer (M3) must keep the arrival order of the files; incremental loading with late events (M5); pytest's 5 dependencies were not checked against the 14-day rule (they are locked with their hashes).

**Status:** M2 closed on 2026-11-08, merged through the module's pull request.

**Next:** M3, Kafka.

## M3: Kafka (2026-11-09)

**Goal:** publish the generator's events to Kafka, and understand what Kafka guarantees and what it does not.

**What was built**
- Topics (`scripts/init_kafka.py`, run by `make up`): 3 topics with 3 partitions each, replication factor 1, unlimited retention. Idempotent, with a final check. Automatic topic creation is disabled in Redpanda, so a misspelled topic name fails instead of silently creating a new topic.
- Producer (`python -m producer`): each event goes to its topic with the key of the event contract (`customer_id` or `invoice_id`), unchanged. Every delivery is checked through a callback, then `flush()`, then a comparison with the manifest.
- Verification consumer (`python -m consumer`): reads the topics back as a consumer group, commits after processing, stops cleanly on Ctrl+C, and checks counts, one partition per key, order per key and duplicate `event_id`s.
- Python client: confluent-kafka 2.16.0, a wrapper around librdkafka.

**Success criteria**

| Criterion | Result |
|---|---|
| The three topics exist with a justified number of partitions | ✅ 3 partitions each: enough to observe ordering per key and consumer groups; few enough for the volume, a single-core broker and Spark `local[2]` in M4, where each partition becomes a task and output files |
| The number of messages matches the manifest | ✅ 185 / 1,507 / 886 acknowledged by the broker, confirmed by the offsets (481 + 491 + 535 = 1,507 invoices) and by a full read |
| The events of an invoice arrive in order | ✅ 2,296 keys read back in the order they were sent |
| A stopped consumer resumes without losing messages | ✅ Stopped after 1,000 messages; restarted, the same group read the other 1,578; lag 0 |

**Decisions**
- confluent-kafka rather than kafka-python: the industry standard, with the configuration names of the Java client.
- librdkafka defaults overridden on purpose: idempotence on (off by default), murmur2 partitioner like Java clients (CRC32 by default), and `auto.offset.reset=earliest` for the verification consumer (by default a new group starts at the end and sees nothing).
- 3 partitions per topic: sized for throughput and parallelism, not for the number of keys.
- The producer publishes the files topic by topic: Kafka gives no ordering across topics anyway.

**Experiments and lessons**
- Same key, same partition; an offset numbers a partition, not a key. Order holds within a partition only, and the order between partitions changes from one read to the next.
- A group's committed offset is the next offset to read, and it is kept in `__consumer_offsets` after the consumer leaves.
- A silent failure: writing to a misspelled topic with rpk printed neither success nor error. Hence a producer that checks every delivery.
- A consumer killed before its commit left a lag of 78: those messages are read again on restart (at least once). A clean stop commits first.
- The producer run twice reported `OK` both times and doubled the topics (5,156 messages, 2,519 distinct `event_id`s). Producer idempotence only covers resends within one run: duplicates are removed downstream, on `event_id` (M5).
- A bug of ours: a synchronous commit with nothing new to commit fails (`_NO_OFFSET`).

**Still open:** the durability of Redpanda's `dev-container` mode (write caching, no forced disk write) is documented but not tested; a single broker means no replication; no ordering across topics, so silver must accept a payment read before its invoice.

**Status:** M3 closed on 2026-11-09, merged through the module's pull request.

**Next:** M4, Spark and the bronze layer.

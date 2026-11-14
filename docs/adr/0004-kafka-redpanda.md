# ADR 0004: Carry the events through the Kafka API, served by Redpanda

- **Status:** Accepted (M0 for the broker, M3 for the topics and clients)
- **Date:** 2026-11-09, written down on 2026-11-14

## Context

The invoicing application (simulated by the generator) emits events: customers, invoices, payments. The data platform must receive them without slowing the application down, keep them long enough to replay them, and keep the events of one invoice in order (issued, updated, paid, refunded, cancelled).

Kafka is the usual answer: an append-only log per partition, read by any number of consumer groups, each at its own pace. Floci emulates Amazon MSK, but in M0 its broker disappeared after a Floci restart while the API still reported the cluster `ACTIVE` (ADR 0001). The broker also has to fit in the 8 GB shared by the whole stack.

## Decision

1. **Redpanda v26.2.6, one broker, as a Compose service** (1 GiB cap). Redpanda speaks the Kafka protocol in a single binary, with no JVM and no ZooKeeper; clients use standard Kafka libraries and settings. It runs in `dev-container` mode (made for development).
2. **One topic per entity, versioned in the name:** `finance.customers.v1`, `finance.invoices.v1`, `finance.payments.v1`. A breaking change of the event format would get a `v2` topic.
3. **The key is the business identifier of the event contract:** `customer_id` for customers, `invoice_id` for invoices **and** payments. Same key, same partition, so every event of an invoice is read in the order it was written.
4. **3 partitions per topic**, sized for throughput and parallelism (each partition becomes one Spark task), not for the number of keys. Replication factor 1 (one broker). Retention unlimited (`retention.ms=-1`), so the topics can be read again from the start.
5. **No automatic topic creation:** `scripts/init_kafka.py` (run by `make up`) creates the topics and checks them. A misspelled topic name fails instead of creating a new, empty topic.
6. **Producer:** confluent-kafka 2.16.0 (librdkafka), idempotence on, `murmur2_random` partitioner (the Java clients' hash, so the same key lands on the same partition whatever the client language). Every delivery is checked with a callback, then `flush()`, then the counts are compared with the generator's manifest.
7. **At least once, duplicates removed downstream:** a consumer can read a message twice after a crash, and a producer run twice publishes twice. Silver keeps one row per `event_id` (ADR 0007).

## What was proven (M3)

| Check | Result |
|---|---|
| Counts match the manifest | ✅ 185 customers, 1,507 invoices, 886 payments acknowledged; confirmed by the offsets (481 + 491 + 535 = 1,507) and by a full read |
| Order per invoice | ✅ 2,296 keys read back in the order they were sent, each key in a single partition |
| A stopped consumer resumes without loss | ✅ stopped after 1,000 messages; the same group read the other 1,578; lag 0 |
| A consumer killed before its commit | ✅ lag of 78 left; those messages are read again at restart (at least once) |
| Producer run twice | ✅ reported `OK` both times and doubled the topics (5,156 messages, 2,519 distinct `event_id`s): producer idempotence only covers retries within one run |
| Misspelled topic | ✅ refused; `rpk` printed neither success nor error, which is why our producer checks every delivery itself |

## Limits

- **One broker, replication factor 1:** losing the disk loses the data; nothing is replicated.
- **`dev-container` mode** caches writes and does not force them to disk: durability after a crash is documented as weaker, and was not tested.
- **No order across topics:** a payment can be read before its invoice. Silver must accept it.
- **No schema validation at publish time:** the contract is a document (`docs/event-contract.md`); the Schema Registry is running but not used.
- **No security:** no TLS, no authentication, no access control.
- **Kafka timestamps are publication times:** the producer publishes the generated files at once, so late arrivals cannot be measured from Kafka (M7).

## When not to use it

- **One producer, one consumer, a daily file:** dropping the file on S3 is simpler and enough; Kafka pays off with several consumers, replay, and a continuous flow.
- **Order across entities is required:** Kafka orders within a partition only; one topic for all events of an invoice, or the database's own change log, would be needed.
- **On AWS:** Amazon MSK keeps the same protocol and clients, without running brokers ourselves.

## Alternatives considered

| Alternative | Why not |
|---|---|
| Floci's MSK | The broker did not survive a restart, and its address was a dynamic IP (ADR 0001) |
| Apache Kafka (KRaft mode) | The same protocol and a valid choice; Redpanda is a single process with no JVM, lighter on an 8 GB machine |
| kafka-python | A pure Python client; confluent-kafka wraps librdkafka, the most used native client, with the same setting names as the Java client |
| Writing files straight to S3 | No replay, no consumer groups, no decoupling: the point of this layer is to show them |

# sme-finance-lakehouse

A local data platform for a fictional small-business accounting scenario, built on a
medallion architecture (bronze, silver, gold) over S3-compatible object storage.
Storage and compute are decoupled, so each can scale on its own as a company grows.
Kafka, Spark and Parquet are used even though the data volume is small: the goal is to
show how these tools work, where their bottlenecks are, and when they are not worth it.
dbt handles the transformations. No cloud account: AWS services are emulated locally with Floci.

**Work in progress**: built module by module.

## Stack

Kafka (Redpanda) → Spark → S3 + Parquet → dbt (DuckDB) → Glue + Athena, orchestrated by Airflow.
S3, Glue and Athena are emulated by [Floci](docs/adr/0001-floci.md).

## Progress

- [x] M0 — Feasibility spike: Floci emulates S3, Glue and Athena; DuckDB and Spark read and write Parquet on it; Redpanda replaces Floci's Kafka (MSK), which did not survive restarts. See [ADR 0001](docs/adr/0001-floci.md).
- [ ] M1 — Repository foundations (in progress)

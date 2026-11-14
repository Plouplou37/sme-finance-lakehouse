# ADR 0005: Ingest Kafka into bronze with Spark Structured Streaming

- **Status:** Accepted (M4; Spark Connect added in M8)
- **Date:** 2026-11-11, written down on 2026-11-14

## Context

Every Kafka message must land in the lake's bronze layer as Parquet: none lost, none written twice, and without any business transformation, so that silver can be rebuilt from bronze whenever a rule changes. The job runs once a day from Airflow, and must resume where it stopped after a crash.

The volume is small (2,578 events for the default dataset). Spark is chosen to show how a distributed engine handles streaming state and files on S3, knowing it is heavier than needed here.

## Decision

1. **Spark 4.2.0 (Java 17)** in our own image, with the s3a connector (hadoop-aws 3.5.0, AWS SDK 2.35.4) and the Kafka connector (spark-sql-kafka 4.2.0 and the dependencies listed in its POM), every jar checked against its Maven Central SHA-1.
2. **Structured Streaming with `Trigger.AvailableNow`:** each run reads everything that arrived since the last run, in micro-batches of at most 1,000 messages, then stops. An incremental batch, without a cluster running all day.
3. **Checkpoint on S3** (`s3a://sme-lake/checkpoints/bronze`): the Kafka offsets of each batch are written before the batch and committed after it. Together with the file sink's own log (`_spark_metadata`), a batch replayed after a crash is not written twice.
4. **Raw rows:** the message stays as text (`raw_json`, never parsed), with its Kafka key, topic, partition, offset and timestamp, plus `ingested_at` and `ingestion_date`. A change in the application's JSON can never break ingestion.
5. **Hive-style folders:** `bronze/topic=<topic>/ingestion_date=<date>/`. The topic and the date become columns when reading, and a filter on them skips whole folders.
6. **Local mode** (`local[2]`, one JVM, 1.5 GiB cap): no cluster for this volume.
7. **From Airflow, through Spark Connect (M8):** a `spark-connect` service runs the Spark server; the Airflow task sends the job with the thin `pyspark-client` (no Java in Airflow, no Docker socket).
8. **A separate check job** (`check_bronze.py`) reads bronze without trusting the ingestion job, and prints the physical plan of its main query.

## What was proven (M4)

| Check | Result |
|---|---|
| Bronze matches Kafka | ✅ 2,578 rows; in each of the 9 partitions, offsets from 0 to the last one, no gap, no duplicate |
| A run without new messages | ✅ `0 messages in 0 batches`, bronze unchanged |
| Crash between writing a batch and committing it | ✅ simulated by deleting the commit marker of batch 2: only batch 2 was replayed, with the same offsets, and the sink skipped the files it already had |
| Columnar reads | ✅ the plan of the check query reads only `kafka_partition` and `kafka_offset`: `raw_json`, the largest column, is never read from disk |
| From Airflow | ✅ `ingest_bronze` through Spark Connect in every daily run (M8) |

## Limits

- **Small files:** 27 files for 2,578 rows (3 batches × 9 partitions). At scale they need compacting, or fewer, larger batches.
- **Other readers do not know `_spark_metadata`:** DuckDB or Athena list the whole folder and would see the files of an unfinished batch. Silver removes duplicates on `event_id` anyway.
- **A checkpoint and its output folder are a pair:** they are reset together, or the job writes to a new folder.
- **Progress lives in the checkpoint, not in Kafka:** Kafka's lag tools do not see how far Spark has read.
- **Not tested:** deleting the checkpoint while keeping `_spark_metadata`; an expired Kafka retention (`failOnDataLoss`); several machines; larger volumes and their memory use.
- **About ten seconds of start-up for 2,578 messages:** at this volume Spark costs more than it brings.

## When not to use it

- **At this volume:** a small Python consumer writing Parquet with DuckDB or pyarrow, or a ready-made S3 sink (Kafka Connect, Amazon Data Firehose from MSK), would be simpler. Spark is worth it when the volume needs several machines, or when the ingestion also joins or aggregates large data.
- **For latency in seconds:** a continuously running stream (Spark or Flink) instead of a daily batch.

## Alternatives considered

| Alternative | Why not |
|---|---|
| A Python consumer writing Parquet | The simplest here, but exactly-once files and resume would have to be written by hand; the point was to see how Spark's checkpoint does it |
| Kafka Connect with an S3 sink | Standard and code-free, but one more JVM service; not tested |
| A streaming query running all day | Resources held all day for a once-a-day need |
| Flink | True event-at-a-time streaming, not needed for a daily batch |
| `DockerOperator` starting a Spark container from Airflow | Needs the Docker socket, root on the host (ADR 0003) |

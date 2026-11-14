"""Copy the three Kafka topics into the bronze layer, unchanged (M4, run through Spark Connect in M8).

Two ways to run it:
    docker compose run --rm spark          # M4: spark-submit starts a local Spark inside the container
    SPARK_REMOTE=sc://spark-connect:15002  # M8: a thin client (Airflow) sends the job to the Spark Connect
    python ingest_bronze.py                #     server over the network; no Java, no Spark on the client

Reads every message that arrived in Kafka since the last run (Trigger.AvailableNow), writes it to
s3a://sme-lake/bronze/ as Parquet, then stops. The checkpoint remembers which offsets are written:
a second run with no new message writes nothing, and a run stopped halfway resumes where it was.

Bronze keeps each message exactly as received (the JSON as text, not parsed), plus its Kafka
coordinates: a schema change in the application can never break this job, and silver can be
rebuilt from bronze at any time.

With Spark Connect, the Kafka address and the S3 paths are resolved by the SERVER (inside the Docker
network): redpanda:9092 and s3a://... work the same in both modes.
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]  # redpanda:9092 inside the Docker network
TOPICS = "finance.customers.v1,finance.invoices.v1,finance.payments.v1"
BRONZE_PATH = "s3a://sme-lake/bronze"
CHECKPOINT_PATH = "s3a://sme-lake/checkpoints/bronze"
# At most this many messages per micro-batch: the 2,578 events are written in 3 batches,
# which makes a stop in the middle of a run possible to observe.
MAX_OFFSETS_PER_BATCH = 1000
REMOTE = bool(os.environ.get("SPARK_REMOTE"))  # set: Spark Connect client; unset: classic spark-submit


def read_progress(progress, name):
    """One field of a micro-batch progress report. It is an object with attributes or a dict,
    depending on the PySpark mode and version; with Spark Connect, numInputRows is only an attribute."""
    value = getattr(progress, name, None)
    if value is None and isinstance(progress, dict):
        value = progress.get(name)
    return value


def main() -> None:
    # With SPARK_REMOTE set, getOrCreate() connects to the Spark Connect server instead of starting Spark.
    spark = SparkSession.builder.appName("ingest_bronze").getOrCreate()
    if not REMOTE:
        spark.sparkContext.setLogLevel("WARN")  # keep the output readable (no SparkContext with Spark Connect)

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPICS)
        # First run only: start from the oldest message. Afterwards, the checkpoint decides.
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_BATCH)
        .load()
    )

    # No parsing and no business rule: the message as text, plus where it came from in Kafka.
    bronze = kafka.select(
        F.col("value").cast("string").alias("raw_json"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),  # when the producer sent it, not the event_time
        F.current_timestamp().alias("ingested_at"),  # when this micro-batch ran
    ).withColumn("ingestion_date", F.to_date("ingested_at"))

    query = (
        bronze.writeStream.format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        # One folder per topic and per day: bronze/topic=finance.invoices.v1/ingestion_date=2026-11-11/
        .partitionBy("topic", "ingestion_date")
        .outputMode("append")
        .trigger(availableNow=True)  # process everything available now, then stop
        .start()
    )
    query.awaitTermination()

    # Every batch is listed, even those with 0 rows, so that a replayed batch stays visible.
    batches = list(query.recentProgress)
    for progress in batches:
        rows = read_progress(progress, "numInputRows")
        print("Batch {}: {} messages read".format(read_progress(progress, "batchId"), rows))
    total = sum(read_progress(p, "numInputRows") or 0 for p in batches)
    print("Done: {} messages in {} batches".format(total, len(batches)))

    if "--explain" in sys.argv:
        query.explain()  # physical plan of the last batch that ran

    spark.stop()


if __name__ == "__main__":
    main()

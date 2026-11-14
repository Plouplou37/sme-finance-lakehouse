"""M8 spike: can a Spark Connect client run our Kafka -> Parquet streaming job on a remote Spark server?

Run from the repository root, with the spark-connect service up:
    docker compose up -d spark-connect
    uv run --with pyspark-client==4.2.0 python spikes/m8/connect_ingest.py

The client is a thin Python package (no Java, no Spark): it only sends the plan of the job over the
network to sc://localhost:15002, and the server runs it. Kafka and S3 addresses are therefore the ones the
SERVER sees (redpanda:9092, s3a://...). Writes to s3a://sme-lake/bench/connect-spike/ with its own
checkpoint, so the real bronze layer and its checkpoint are untouched.
Expected: all 2,578 messages read from Kafka and written, in a few micro-batches.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OUTPUT_PATH = "s3a://sme-lake/bench/connect-spike/bronze"
CHECKPOINT_PATH = "s3a://sme-lake/bench/connect-spike/checkpoint"


def field(progress, name):
    return progress[name] if isinstance(progress, dict) else getattr(progress, name)


def main() -> None:
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    print("Connected to Spark", spark.version)

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "redpanda:9092")
        .option("subscribe", "finance.customers.v1,finance.invoices.v1,finance.payments.v1")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )
    bronze = kafka.select(
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    query = (
        bronze.writeStream.format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    for progress in query.recentProgress:
        print("Batch {}: {} messages".format(field(progress, "batchId"), field(progress, "numInputRows")))
    print("Rows read back from the output:", spark.read.parquet(OUTPUT_PATH).count())
    spark.stop()


if __name__ == "__main__":
    main()

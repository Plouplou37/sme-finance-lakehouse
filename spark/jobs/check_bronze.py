"""Check the bronze layer against Kafka, without trusting the ingestion job (M4).

Run from the repository root:
    docker compose run --rm spark /opt/spark/bin/spark-submit /jobs/check_bronze.py

Reads bronze as a batch (a Spark reader uses _spark_metadata, so it sees only committed files) and checks:
- no message twice: each (topic, kafka_partition, kafka_offset) appears once;
- no message missing: in each Kafka partition, offsets go from 0 to the last one without a gap
  (our topics are neither compacted nor transactional, so Kafka offsets have no holes);
- the event_ids: distinct count and copies, to compare with the generator's manifest.
Then prints the physical plan of the main query, to read and comment.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_PATH = "s3a://sme-lake/bronze"


def main() -> None:
    spark = SparkSession.builder.appName("check_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.parquet(BRONZE_PATH)
    print(f"Files read: {len(bronze.inputFiles())}")

    # One line per Kafka partition: how many rows, which offsets, how many distinct offsets.
    per_partition = (
        bronze.groupBy("topic", "kafka_partition")
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct("kafka_offset").alias("distinct_offsets"),
            F.min("kafka_offset").alias("first_offset"),
            F.max("kafka_offset").alias("last_offset"),
        )
        .orderBy("topic", "kafka_partition")
    )
    rows = per_partition.collect()  # 9 small rows: safe to bring back to the driver

    problems = []
    for row in rows:
        print(
            "{} p{}: {} rows, offsets {}..{}".format(
                row["topic"], row["kafka_partition"], row["rows"], row["first_offset"], row["last_offset"]
            )
        )
        if row["distinct_offsets"] != row["rows"]:
            problems.append("{} p{}: some offsets written twice".format(row["topic"], row["kafka_partition"]))
        if row["first_offset"] != 0 or row["last_offset"] + 1 != row["rows"]:
            problems.append("{} p{}: missing offsets".format(row["topic"], row["kafka_partition"]))

    totals = bronze.groupBy("topic").count().orderBy("topic").collect()
    for row in totals:
        print("{}: {} rows".format(row["topic"], row["count"]))

    event_ids = bronze.select(F.get_json_object("raw_json", "$.event_id").alias("event_id"))
    total = event_ids.count()
    distinct = event_ids.distinct().count()
    print(f"Rows: {total}, distinct event_ids: {distinct}, extra copies: {total - distinct}")

    if problems:
        raise SystemExit(f"Bronze check failed: {problems}")
    print("OK: every Kafka offset is in bronze exactly once")

    print("\nPhysical plan of the per-partition query:")
    per_partition.explain()

    spark.stop()


if __name__ == "__main__":
    main()

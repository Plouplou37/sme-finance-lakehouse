"""M0 test 7: Spark reads and writes Parquet on Floci's S3 through s3a."""

from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE = "s3a://sme-lake/m0/duckdb/"          # Parquet written by DuckDB in test 3
TARGET = "s3a://sme-lake/m0/spark/amounts/"   # where Spark writes its own copy

EXPECTED_ROWS = 1000
EXPECTED_TOTAL = Decimal("749250.00")


def check(df, label):
    """Count rows and sum amounts in ONE Spark job, then compare with the expected values."""
    # One action (collect) computes both aggregates; [0] = the single result row.
    metrics = df.agg(
        F.count("*").alias("nb_rows"),
        F.sum("amount").alias("total"),
    ).collect()[0]
    print(f"[{label}] rows: {metrics['nb_rows']}, total: {metrics['total']}")

    # assert <condition>, <message shown if the condition is false>
    assert metrics["nb_rows"] == EXPECTED_ROWS, f"[{label}] expected {EXPECTED_ROWS} rows, got {metrics['nb_rows']}"
    assert metrics["total"] == EXPECTED_TOTAL, f"[{label}] expected total {EXPECTED_TOTAL}, got {metrics['total']}"


spark = SparkSession.builder.appName("m0-spark-s3").getOrCreate()
spark.sparkContext.setLogLevel("WARN")   # fewer Spark log lines from here on
print("Spark version:", spark.version)

# Read the Parquet written by DuckDB (lazy: only lists the files and reads the schema).
df = spark.read.parquet(SOURCE)
df.printSchema()
check(df, "source written by DuckDB")

# Write a copy; "overwrite" replaces a previous run instead of adding files next to it.
df.write.mode("overwrite").parquet(TARGET)

# Read back what Spark just wrote, and check it again.
df_target = spark.read.parquet(TARGET)
check(df_target, "copy written by Spark")

print("OK")
spark.stop()

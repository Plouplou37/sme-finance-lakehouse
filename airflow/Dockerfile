# Airflow 3.3.2 (Python 3.12) with what its tasks need, and nothing more:
# - dbt (+ DuckDB) to build silver and gold;
# - the Spark Connect client, to send the ingestion job to the spark-connect service over the network.
# No Java, no Spark and no Docker socket: Airflow only orchestrates, the computing happens elsewhere.
# 3.3.2 is the newest Airflow release at least 14 days old (3.4.0 was 5 days old on 2026-11-11).
FROM apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1

# Each tool gets its own virtual environment: dbt, the Spark client and Airflow pin different versions of
# the same libraries, and installing them together would risk breaking Airflow itself.
# pandas < 3: the Spark client warns that it does not fully support pandas 3 yet (seen in the M8 spike).
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
        "dbt-core==1.12.6" "dbt-duckdb==1.11.1" "duckdb==1.5.6" \
 && python -m venv /home/airflow/spark-client-venv \
 && /home/airflow/spark-client-venv/bin/pip install --no-cache-dir \
        "pyspark-client==4.2.0" "pandas<3"

# The folder of the airflow-warehouse volume must exist in the image, owned by the airflow user: a new named
# volume copies the owner of the folder it is mounted on. Without it, the volume belongs to root and dbt
# cannot create its DuckDB file ("Permission denied", seen on the first M8 run).
RUN mkdir -p /opt/airflow/warehouse

# Spark 4.2.0 with the s3a connector (S3 on Floci) and the Kafka connector (Redpanda), for the M4 bronze job.
FROM apache/spark:4.2.0-scala2.13-java17-python3-ubuntu@sha256:84a4eedb1abcf36a90808d5a1310e3e910b78c85d85aaa1599e31af5f862ed59

# Spark reaches S3 through Hadoop's s3a connector, which the image does not include. Versions must match exactly:
# Spark 4.2.0 bundles Hadoop 3.5.0 -> hadoop-aws 3.5.0 -> AWS SDK v2 bundle 2.35.4 (+ analytics-accelerator 1.3.1).
# The jars are baked into the image (the SDK bundle alone is ~654 MiB) instead of being downloaded at every start.
# Each download is checked against the SHA-1 published by Maven Central (values copied on 2026-11-07):
# if one byte differs, sha1sum fails and so does the build.
# This block is kept identical to the M0 spike, so Docker reuses its cached layer (no new 654 MiB download).
USER root
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; \
    cd /opt/spark/jars; \
    wget -q "${MAVEN}/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar"; \
    wget -q "${MAVEN}/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar"; \
    wget -q "${MAVEN}/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar"; \
    printf '%s  %s\n' \
        9e594525d264c0db653c7f68da98b245f7d61ea5 hadoop-aws-3.5.0.jar \
        7252265e3970b214708e68a8b74a8fa8c875af1e bundle-2.35.4.jar \
        6c9bd0f6c440c9a78e82d272f5f0252d942419f6 analyticsaccelerator-s3-1.3.1.jar \
    | sha1sum -c -

# Kafka source for Structured Streaming. spark-sql-kafka 4.2.0 needs, at the versions its POM declares:
# spark-token-provider-kafka 4.2.0, kafka-clients 3.9.2 and commons-pool2 2.13.1. Its other dependencies
# (spark-tags, hadoop-client-runtime, snappy, lz4, zstd, slf4j...) already ship with Spark.
# SHA-1 values from Maven Central's .sha1 files, copied on 2026-11-11.
RUN set -eux; \
    cd /opt/spark/jars; \
    wget -q "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.2.0/spark-sql-kafka-0-10_2.13-4.2.0.jar"; \
    wget -q "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.2.0/spark-token-provider-kafka-0-10_2.13-4.2.0.jar"; \
    wget -q "${MAVEN}/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar"; \
    wget -q "${MAVEN}/org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar"; \
    printf '%s  %s\n' \
        98f199d5da59b1e152dae941cc5037a6495464ad spark-sql-kafka-0-10_2.13-4.2.0.jar \
        807ceb26e2fa509259e74a202d5f2d790446f515 spark-token-provider-kafka-0-10_2.13-4.2.0.jar \
        a06edbaa01458ed2e8cf1be69a18c2f231fda6e0 kafka-clients-3.9.2.jar \
        cf7e8c0242d4c483c42c083b8188ab360ab60f9a commons-pool2-2.13.1.jar \
    | sha1sum -c -

# Spark settings (local mode, s3a -> Floci), read by spark-submit at startup.
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Back to the image's unprivileged user (uid 185).
USER spark

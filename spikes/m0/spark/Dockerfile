# M0 test 7: Spark 4.2.0 reads and writes Parquet on Floci's S3 through the s3a connector.
FROM apache/spark:4.2.0-scala2.13-java17-python3-ubuntu@sha256:84a4eedb1abcf36a90808d5a1310e3e910b78c85d85aaa1599e31af5f862ed59

# Spark reaches S3 through Hadoop's s3a connector, which the image does not include. Versions must match exactly:
# Spark 4.2.0 bundles Hadoop 3.5.0 -> hadoop-aws 3.5.0 -> AWS SDK v2 bundle 2.35.4 (+ analytics-accelerator 1.3.1).
# The jars are baked into the image (the SDK bundle alone is ~654 MiB) instead of being downloaded at every start.
# Each download is checked against the SHA-1 published by Maven Central (values copied on 2026-11-07):
# if one byte differs, sha1sum fails and so does the build.
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

# Spark settings (local mode, s3a -> Floci), read by spark-submit at startup.
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Back to the image's unprivileged user (uid 185).
USER spark

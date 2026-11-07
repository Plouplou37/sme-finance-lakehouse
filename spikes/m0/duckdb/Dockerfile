# Throwaway image for M0 test 3: DuckDB writes and reads Parquet on Floci's S3.
FROM python:3.12.15-slim@sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704

# Stay on the DuckDB 1.5 line: dbt-duckdb only tests DuckDB 2.0 as a pre-release (checked 2026-11-07).
RUN pip install --no-cache-dir duckdb==1.5.6

# Download the httpfs extension (S3 support) at build time,
# so the test does not need to reach extensions.duckdb.org at run time.
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs')"

WORKDIR /work

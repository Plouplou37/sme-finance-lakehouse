"""M0 test 3: DuckDB writes then reads a Parquet file on Floci's S3, from another container."""

import os

import duckdb

TARGET = "s3://sme-lake/m0/duckdb/amounts.parquet"

con = duckdb.connect()  # in-memory database: nothing is stored inside the container
con.sql("LOAD httpfs")

# Credentials come from environment variables set in docker-compose.yml, never hard-coded.
con.sql(f"""
    CREATE SECRET floci (
        TYPE s3,
        KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
        SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
        REGION 'us-east-1',
        ENDPOINT '{os.environ["S3_ENDPOINT"]}',  -- host:port only; USE_SSL decides http vs https
        URL_STYLE 'path',                        -- http://floci:4566/sme-lake/... (bucket in the path)
        USE_SSL false                            -- Floci speaks plain HTTP
    )
""")

# 1) Write: 1,000 rows. Amounts are DECIMAL, never FLOAT: money must not suffer rounding errors.
con.sql(f"""
    COPY (
        SELECT range AS id, (range * 1.5)::DECIMAL(12, 2) AS amount
        FROM range(1000)
    )
    TO '{TARGET}' (FORMAT parquet)
""")
print(f"written: {TARGET}")

# 2) Read back from S3 and check against values computed in advance.
result = con.sql(f"SELECT count(*) AS n_rows, sum(amount) AS total FROM read_parquet('{TARGET}')")
result.show()

n_rows, total = result.fetchone()
assert n_rows == 1000, f"expected 1000 rows, got {n_rows}"
assert total == 749250, f"expected total 749250, got {total}"
print("OK: 1000 rows, total 749250.00")

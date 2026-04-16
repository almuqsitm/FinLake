"""
src/schemas/silver_ohlcv_schema.py

Defines the PySpark StructType schema for the Silver OHLCV table.

Why hard-code schemas instead of inferring them?
  Schema inference (letting Spark guess the types from the data) is
  convenient in development but dangerous in production:
  - A single null value in a numeric column makes Spark infer 'string'
  - A missing file makes Spark infer an empty schema
  - Schema changes in Bronze silently break Silver

  Explicit schemas fail loudly and immediately if the source data
  doesn't match. "Fail fast" is a core principle of production pipelines.

  This is also what GS means by "schema enforcement" — a contractual
  guarantee between the Bronze data producer and Silver consumer.
"""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ─────────────────────────────────────────────
# Silver OHLCV schema
# Applied when reading Bronze JSON and writing to Iceberg
# ─────────────────────────────────────────────
SILVER_OHLCV_SCHEMA = StructType([
    StructField("symbol",       StringType(),    nullable=False),  # e.g. "AAPL"
    StructField("trade_date",   DateType(),      nullable=False),  # e.g. 2024-01-15
    StructField("open",         DoubleType(),    nullable=True),   # Opening price
    StructField("high",         DoubleType(),    nullable=True),   # Intraday high
    StructField("low",          DoubleType(),    nullable=True),   # Intraday low
    StructField("close",        DoubleType(),    nullable=False),  # Closing price (required)
    StructField("volume",       LongType(),      nullable=True),   # Shares traded
    StructField("ingested_at",  TimestampType(), nullable=True),   # When Bronze was written
    StructField("processed_at", TimestampType(), nullable=True),   # When Silver job ran
])

# ─────────────────────────────────────────────
# Silver Tick schema (from streaming Bronze Avro)
# ─────────────────────────────────────────────
SILVER_TICK_SCHEMA = StructType([
    StructField("symbol",           StringType(),    nullable=False),
    StructField("event_timestamp",  TimestampType(), nullable=False),
    StructField("price",            DoubleType(),    nullable=False),
    StructField("bid",              DoubleType(),    nullable=True),
    StructField("ask",              DoubleType(),    nullable=True),
    StructField("volume",           LongType(),      nullable=True),
    StructField("exchange",         StringType(),    nullable=True),
    StructField("trade_condition",  StringType(),    nullable=True),
    StructField("processed_at",     TimestampType(), nullable=True),
])

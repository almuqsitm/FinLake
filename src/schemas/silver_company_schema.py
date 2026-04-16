"""
src/schemas/silver_company_schema.py

Defines the PySpark StructType for the company dimension table in Silver.

This schema underpins the SCD Type 2 implementation. The key fields
that make SCD2 work are:

  effective_from  — date this version of the record became active
  effective_to    — date this version expired (NULL = currently active)
  is_current      — boolean flag for fast filtering of current records
  record_hash     — MD5 hash of the business attributes; if this changes
                    between ingestion runs, we know to create a new SCD2 row

The record_hash pattern is the standard production approach: instead of
comparing every column individually, hash them all into one value.
If hash(today) != hash(yesterday), something changed.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SILVER_COMPANY_SCHEMA = StructType([
    # ── Business key ─────────────────────────────────────────────
    StructField("symbol",           StringType(),    nullable=False),

    # ── Business attributes (any change triggers a new SCD2 row) ─
    StructField("short_name",       StringType(),    nullable=True),
    StructField("long_name",        StringType(),    nullable=True),
    StructField("sector",           StringType(),    nullable=True),  # SCD2 trigger
    StructField("industry",         StringType(),    nullable=True),  # SCD2 trigger
    StructField("exchange",         StringType(),    nullable=True),
    StructField("currency",         StringType(),    nullable=True),
    StructField("country",          StringType(),    nullable=True),
    StructField("state",            StringType(),    nullable=True),
    StructField("city",             StringType(),    nullable=True),
    StructField("full_time_employees", IntegerType(), nullable=True),
    StructField("market_cap",       LongType(),      nullable=True),
    StructField("business_summary", StringType(),    nullable=True),
    StructField("website",          StringType(),    nullable=True),

    # ── SCD Type 2 control columns ───────────────────────────────
    StructField("effective_from",   DateType(),      nullable=False),
    StructField("effective_to",     DateType(),      nullable=True),   # NULL = current
    StructField("is_current",       BooleanType(),   nullable=False),
    StructField("record_hash",      StringType(),    nullable=False),  # MD5 of attributes

    # ── Audit columns ────────────────────────────────────────────
    StructField("snapshot_date",    DateType(),      nullable=True),
    StructField("processed_at",     TimestampType(), nullable=True),
])

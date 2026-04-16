"""
src/transformation/scd2_company_dim.py

Silver Layer — SCD Type 2 Company Dimension

Implements Slowly Changing Dimension Type 2 for the company dimension table.
This preserves the full history of changes to company attributes over time.

How SCD Type 2 works:
  ┌──────────┬──────────┬──────────────┬──────────────┬────────────┐
  │ symbol   │ sector   │ effective_from│ effective_to │ is_current │
  ├──────────┼──────────┼──────────────┼──────────────┼────────────┤
  │ GOOGL    │ Tech     │ 2015-01-01   │ 2018-09-30   │ false      │  ← old
  │ GOOGL    │ Comm Svc │ 2018-10-01   │ NULL         │ true       │  ← current
  └──────────┴──────────┴──────────────┴──────────────┴────────────┘

  When sector changes: close old row (set effective_to = today, is_current = false)
                        insert new row (effective_from = today, is_current = true)
  When nothing changes: do nothing (no new row needed)

Implementation approach:
  We use a record_hash (MD5 of business attributes) to detect changes.
  Instead of comparing 12 columns individually, hashing them all into one
  value means: hash(today) != hash(yesterday) → something changed.

  The merge logic:
    1. Read current Silver company_dim (existing history)
    2. Read Bronze latest.json (today's snapshot from ingestor)
    3. Hash both and compare
    4. For changed records: expire old row, insert new row
    5. For new symbols (first time seen): insert as new current record
    6. For unchanged records: leave them alone
"""

import hashlib
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)

BRONZE_BUCKET = "s3a://finlake"
SILVER_DB = "finlake.silver"

# Columns that, when changed, trigger a new SCD2 record.
# Market cap and employee count are excluded — they change constantly
# and we don't want a new history row every day for those.
SCD2_TRACKED_COLUMNS = [
    "short_name", "sector", "industry", "exchange", "currency", "country",
]


def compute_record_hash(df: DataFrame) -> DataFrame:
    """
    Adds a record_hash column = MD5 of all SCD2-tracked business attributes.

    MD5 is used here for speed (not security). The hash serves as a
    change detection fingerprint, not a cryptographic guarantee.

    F.concat_ws concatenates all tracked column values with '|' separator,
    then F.md5 hashes the resulting string to a 32-char hex string.
    """
    hash_input = F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in SCD2_TRACKED_COLUMNS])
    return df.withColumn("record_hash", F.md5(hash_input))


def read_bronze_company(spark: SparkSession) -> DataFrame:
    """
    Reads the latest company metadata snapshot from Bronze.
    This was written by company_ingestor.py as bronze/company/{SYMBOL}/latest.json
    """
    logger.info("Reading Bronze company metadata...")
    df = spark.read.json(f"{BRONZE_BUCKET}/bronze/company/*/latest.json")

    # Rename columns to match Silver schema (snake_case)
    df = (
        df
        .withColumnRenamed("shortName",          "short_name")
        .withColumnRenamed("longName",           "long_name")
        .withColumnRenamed("longBusinessSummary", "business_summary")
        .withColumnRenamed("fullTimeEmployees",  "full_time_employees")
        .withColumnRenamed("marketCap",          "market_cap")
        .withColumn("snapshot_date", F.to_date(F.col("snapshot_date")))
        .withColumn("processed_at",  F.lit(datetime.utcnow().isoformat()).cast("timestamp"))
    )

    return compute_record_hash(df)


def create_company_dim_table(spark: SparkSession) -> None:
    """Creates the company_dim Iceberg table if it doesn't exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_DB}.company_dim (
            symbol              STRING      NOT NULL,
            short_name          STRING,
            long_name           STRING,
            sector              STRING,
            industry            STRING,
            exchange            STRING,
            currency            STRING,
            country             STRING,
            state               STRING,
            city                STRING,
            full_time_employees INT,
            market_cap          BIGINT,
            business_summary    STRING,
            website             STRING,
            effective_from      DATE        NOT NULL,
            effective_to        DATE,
            is_current          BOOLEAN     NOT NULL,
            record_hash         STRING      NOT NULL,
            snapshot_date       DATE,
            processed_at        TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (is_current)
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """)


def apply_scd2_merge(spark: SparkSession, incoming: DataFrame) -> None:
    """
    Core SCD2 merge logic.

    Three scenarios handled:
      A) New symbol never seen before → INSERT as current record
      B) Symbol exists, attributes changed → EXPIRE old, INSERT new
      C) Symbol exists, nothing changed → do nothing

    We implement this without native SQL MERGE (which Iceberg supports)
    to keep it in pure PySpark for testability across Spark versions.
    """
    today = date.today()

    # Try reading existing dimension table
    try:
        existing = spark.table(f"{SILVER_DB}.company_dim")
        current = existing.filter(F.col("is_current") == True)
        logger.info(f"Existing company_dim: {current.count()} current records")
        table_exists = True
    except Exception:
        logger.info("company_dim table doesn't exist yet — first run")
        current = None
        table_exists = False

    if not table_exists or current is None or current.count() == 0:
        # First run: treat all incoming as new
        new_records = (
            incoming
            .withColumn("effective_from", F.lit(today).cast("date"))
            .withColumn("effective_to",   F.lit(None).cast("date"))
            .withColumn("is_current",     F.lit(True))
        )
        create_company_dim_table(spark)
        new_records.writeTo(f"{SILVER_DB}.company_dim").append()
        logger.info(f"Initial load: inserted {new_records.count()} records into company_dim")
        return

    # Join incoming (today's Bronze) with current Silver records
    joined = incoming.alias("new").join(
        current.select("symbol", "record_hash").alias("old"),
        on="symbol",
        how="left",
    )

    # CASE A: New symbols (no match in existing dim)
    new_symbols = (
        joined
        .filter(F.col("old.record_hash").isNull())
        .select("new.*")
        .withColumn("effective_from", F.lit(today).cast("date"))
        .withColumn("effective_to",   F.lit(None).cast("date"))
        .withColumn("is_current",     F.lit(True))
    )

    # CASE B: Changed records (hash mismatch)
    changed = (
        joined
        .filter(
            F.col("old.record_hash").isNotNull() &
            (F.col("new.record_hash") != F.col("old.record_hash"))
        )
    )

    # Expire old records for changed symbols
    changed_symbols = [row.symbol for row in changed.select("new.symbol").collect()]

    if changed_symbols:
        logger.info(f"Detected changes for: {changed_symbols}")
        # Expire existing current records for changed symbols
        spark.sql(f"""
            UPDATE {SILVER_DB}.company_dim
            SET effective_to = DATE('{today}'),
                is_current = false
            WHERE symbol IN ({','.join(f"'{s}'" for s in changed_symbols)})
              AND is_current = true
        """)

        # Insert new current records for changed symbols
        new_from_changes = (
            changed
            .select("new.*")
            .withColumn("effective_from", F.lit(today).cast("date"))
            .withColumn("effective_to",   F.lit(None).cast("date"))
            .withColumn("is_current",     F.lit(True))
        )
        new_from_changes.writeTo(f"{SILVER_DB}.company_dim").append()

    # Insert truly new symbols
    if new_symbols.count() > 0:
        new_symbols.writeTo(f"{SILVER_DB}.company_dim").append()
        logger.info(f"Inserted {new_symbols.count()} new symbols into company_dim")

    logger.info("SCD2 merge complete")


def run_scd2() -> None:
    """Entry point for the SCD Type 2 company dimension job."""
    spark = get_spark_session("SCD2-CompanyDim")
    try:
        incoming = read_bronze_company(spark)
        create_company_dim_table(spark)
        apply_scd2_merge(spark, incoming)
        logger.info("SCD Type 2 company dimension refresh complete")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_scd2()

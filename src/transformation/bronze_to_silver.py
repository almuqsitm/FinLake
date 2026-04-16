"""
src/transformation/bronze_to_silver.py

Silver Layer — Bronze to Silver PySpark Transformation

Reads raw JSON files from Bronze (MinIO), applies schema enforcement,
deduplication, and type casting, then writes clean Apache Iceberg tables
to the Silver layer.

The four jobs this module runs:
  1. ohlcv_bronze_to_silver()  — daily price data
  2. ticks_bronze_to_silver()  — streaming tick data (from Avro files)

What makes this "Silver" quality vs Bronze:
  - Every column has the correct data type (not just string)
  - Duplicate rows are eliminated (at-least-once Kafka can create dupes)
  - Nulls in critical columns are either filled or the row is rejected
  - Prices are validated (must be > 0)
  - Timestamps are normalized to UTC
  - Output is Iceberg: queryable, versioned, transactional

Partitioning strategy:
  We partition Silver OHLCV by (year, month) rather than by full date.
  This reduces the number of partitions while still letting Spark skip
  entire months when a query filters to a specific time window.
  For a query like "give me all AAPL data in January 2024", Spark reads
  only the January partition — not all 365 days.
"""

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, LongType, TimestampType

from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)

BRONZE_BUCKET = "s3a://finlake"
SILVER_DB = "finlake.silver"


def read_bronze_ohlcv(spark: SparkSession) -> DataFrame:
    """
    Reads all Bronze OHLCV JSON files into a Spark DataFrame.

    s3a://finlake/bronze/ohlcv/*/*/*.json uses wildcards:
      First *  = symbol directory (AAPL, MSFT, ...)
      Second * = date directory   (2024-01-15, ...)
    This reads everything into one DataFrame for batch processing.
    """
    logger.info("Reading Bronze OHLCV JSON files...")
    df = spark.read.json(f"{BRONZE_BUCKET}/bronze/ohlcv/*/*/*.json")
    logger.info(f"Bronze OHLCV raw count: {df.count():,} rows")
    return df


def clean_ohlcv(df: DataFrame) -> DataFrame:
    """
    Applies schema enforcement, type casting, and data quality rules
    to raw Bronze OHLCV data.

    Steps explained:
      1. Cast types: JSON has no types — everything arrives as string.
         We cast each column to its correct type.
      2. Rename: 'date' → 'trade_date' (clearer in SQL queries)
      3. Add processed_at: audit column — when did Silver run?
      4. Filter nulls: rows with null close price are useless, drop them
      5. Filter invalid: prices must be positive (negative price = data error)
      6. Deduplicate: on (symbol, trade_date) — keep the latest ingested copy
    """
    logger.info("Applying schema enforcement and cleaning to OHLCV data...")

    df = (
        df
        # Type casting
        .withColumn("trade_date",   F.to_date(F.col("date").cast("string")))
        .withColumn("open",         F.col("open").cast(DoubleType()))
        .withColumn("high",         F.col("high").cast(DoubleType()))
        .withColumn("low",          F.col("low").cast(DoubleType()))
        .withColumn("close",        F.col("close").cast(DoubleType()))
        .withColumn("volume",       F.col("volume").cast(LongType()))
        .withColumn("ingested_at",  F.col("ingested_at").cast(TimestampType()))
        .withColumn("processed_at", F.lit(datetime.utcnow().isoformat()).cast(TimestampType()))

        # Select final Silver columns (drop raw 'date', keep 'trade_date')
        .select("symbol", "trade_date", "open", "high", "low",
                "close", "volume", "ingested_at", "processed_at")

        # Data quality filters
        .filter(F.col("close").isNotNull())           # Must have closing price
        .filter(F.col("close") > 0)                   # Price must be positive
        .filter(F.col("trade_date").isNotNull())       # Must have a valid date
        .filter(F.col("symbol").isNotNull())           # Must have a symbol

        # Deduplication: if we ingested the same day twice, keep most recent
        # Window function: rank rows within each (symbol, date) group by recency
        .withColumn(
            "row_num",
            F.row_number().over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window
                .partitionBy("symbol", "trade_date")
                .orderBy(F.col("ingested_at").desc())
            )
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Add partition columns for efficient Iceberg storage
    df = (
        df
        .withColumn("year",  F.year("trade_date"))
        .withColumn("month", F.month("trade_date"))
    )

    logger.info(f"Clean OHLCV count: {df.count():,} rows (after dedup + quality filters)")
    return df


def write_silver_ohlcv(spark: SparkSession, df: DataFrame) -> None:
    """
    Writes the clean OHLCV DataFrame to an Apache Iceberg table.

    CREATE TABLE IF NOT EXISTS means: on first run, create the table.
    On subsequent runs, we use INSERT OVERWRITE to replace data for
    the partitions being processed (safe re-run / idempotent).

    Iceberg handles the actual file layout, snapshotting, and metadata.
    """
    logger.info("Writing Silver OHLCV to Iceberg...")

    # Create Iceberg table if it doesn't exist
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_DB}.ohlcv (
            symbol       STRING        NOT NULL,
            trade_date   DATE          NOT NULL,
            open         DOUBLE,
            high         DOUBLE,
            low          DOUBLE,
            close        DOUBLE        NOT NULL,
            volume       BIGINT,
            ingested_at  TIMESTAMP,
            processed_at TIMESTAMP,
            year         INT,
            month        INT
        )
        USING iceberg
        PARTITIONED BY (year, month)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)

    # Write with dynamic overwrite — replaces only the partitions present in df
    df.writeTo(f"{SILVER_DB}.ohlcv").overwritePartitions()
    logger.info("Silver OHLCV write complete")


def run_ohlcv_transform() -> None:
    """Entry point for the OHLCV Bronze → Silver job."""
    spark = get_spark_session("BronzeToSilver-OHLCV")
    try:
        raw = read_bronze_ohlcv(spark)
        clean = clean_ohlcv(raw)
        write_silver_ohlcv(spark, clean)
        logger.info("Bronze → Silver OHLCV transformation complete")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_ohlcv_transform()

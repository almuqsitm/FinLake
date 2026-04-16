"""
src/quality/dq_checks.py

Data Quality Framework

Runs validation checks on Silver layer tables and writes results to
a DQ control table (silver.dq_results) in Iceberg.

Three categories of checks:
  1. COMPLETENESS — are critical columns fully populated?
  2. RANGE       — are numeric values within expected bounds?
  3. FRESHNESS   — is the latest record recent enough?

Why log results to a table instead of just raising exceptions?
  In production, you don't want a single bad record to crash a pipeline
  that processes millions of rows. Instead:
  - Log the failure to dq_results
  - Alert the on-call team
  - Let the pipeline continue (with the bad rows quarantined)
  This pattern is called "quarantine and alert" and it's standard practice
  at firms like GS where pipeline downtime has direct business cost.
"""

from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)
SILVER_DB = "finlake.silver"


# ─────────────────────────────────────────────
# Individual DQ check functions
# Each returns a dict: {check_name, status, details, rows_affected}
# ─────────────────────────────────────────────

def check_completeness(df: DataFrame, column: str, table: str) -> dict:
    """Fails if any row has NULL in a column that should always be populated."""
    null_count = df.filter(F.col(column).isNull()).count()
    total = df.count()
    status = "PASS" if null_count == 0 else "FAIL"
    return {
        "check_name": f"completeness_{table}_{column}",
        "table_name": table,
        "status": status,
        "rows_affected": null_count,
        "total_rows": total,
        "details": f"{null_count} NULLs found in column '{column}' out of {total:,} rows",
    }


def check_price_range(df: DataFrame, table: str) -> dict:
    """Fails if any price column (open/high/low/close) is <= 0."""
    invalid = df.filter(
        (F.col("close") <= 0) |
        (F.col("open") <= 0) |
        (F.col("high") <= 0) |
        (F.col("low") <= 0)
    ).count()
    status = "PASS" if invalid == 0 else "FAIL"
    return {
        "check_name": f"range_{table}_positive_prices",
        "table_name": table,
        "status": status,
        "rows_affected": invalid,
        "total_rows": df.count(),
        "details": f"{invalid} rows with non-positive price values",
    }


def check_high_low_integrity(df: DataFrame, table: str) -> dict:
    """Fails if high < low for any row (physically impossible)."""
    invalid = df.filter(F.col("high") < F.col("low")).count()
    status = "PASS" if invalid == 0 else "FAIL"
    return {
        "check_name": f"integrity_{table}_high_gte_low",
        "table_name": table,
        "status": status,
        "rows_affected": invalid,
        "total_rows": df.count(),
        "details": f"{invalid} rows where high < low (data integrity violation)",
    }


def check_freshness(df: DataFrame, table: str, max_lag_days: int = 3) -> dict:
    """
    Fails if the most recent trade_date is older than max_lag_days.
    Catches pipelines that silently stopped ingesting without errors.
    """
    latest = df.agg(F.max("trade_date")).collect()[0][0]
    if latest is None:
        return {
            "check_name": f"freshness_{table}",
            "table_name": table,
            "status": "FAIL",
            "rows_affected": 0,
            "total_rows": 0,
            "details": "No data found — table may be empty",
        }

    lag = (datetime.utcnow().date() - latest).days
    # Markets are closed weekends, so allow up to 3 days lag
    status = "PASS" if lag <= max_lag_days else "FAIL"
    return {
        "check_name": f"freshness_{table}",
        "table_name": table,
        "status": status,
        "rows_affected": 0,
        "total_rows": df.count(),
        "details": f"Latest trade_date: {latest} | Lag: {lag} days | Threshold: {max_lag_days} days",
    }


def check_duplicate_keys(df: DataFrame, key_cols: list[str], table: str) -> dict:
    """Fails if there are duplicate rows on the natural key."""
    total = df.count()
    distinct = df.select(key_cols).distinct().count()
    dupes = total - distinct
    status = "PASS" if dupes == 0 else "FAIL"
    return {
        "check_name": f"duplicates_{table}_{'_'.join(key_cols)}",
        "table_name": table,
        "status": status,
        "rows_affected": dupes,
        "total_rows": total,
        "details": f"{dupes} duplicate rows on keys {key_cols}",
    }


# ─────────────────────────────────────────────
# Results writer
# ─────────────────────────────────────────────

def write_dq_results(spark: SparkSession, results: list[dict]) -> None:
    """
    Writes DQ check results to the silver.dq_results Iceberg control table.
    This table accumulates results over time — you can chart DQ health trends.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_DB}.dq_results (
            check_name    STRING,
            table_name    STRING,
            status        STRING,
            rows_affected BIGINT,
            total_rows    BIGINT,
            details       STRING,
            run_timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (table_name)
    """)

    now = datetime.utcnow().isoformat()
    for r in results:
        r["run_timestamp"] = now

    df = spark.createDataFrame(results)
    df.writeTo(f"{SILVER_DB}.dq_results").append()

    # Summary log
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    logger.info(f"DQ Results: {passed} PASSED | {failed} FAILED out of {len(results)} checks")

    for r in results:
        icon = "✓" if r["status"] == "PASS" else "✗"
        logger.info(f"  {icon} {r['check_name']}: {r['details']}")


def run_dq_checks() -> None:
    """
    Runs all DQ checks against Silver tables and writes results.
    Called by Airflow after every Silver transformation job.
    """
    spark = get_spark_session("DataQualityChecks")
    try:
        ohlcv = spark.table(f"{SILVER_DB}.ohlcv")
        results = []

        # Completeness checks
        for col in ["symbol", "close", "trade_date"]:
            results.append(check_completeness(ohlcv, col, "silver.ohlcv"))

        # Range and integrity checks
        results.append(check_price_range(ohlcv, "silver.ohlcv"))
        results.append(check_high_low_integrity(ohlcv, "silver.ohlcv"))

        # Freshness check
        results.append(check_freshness(ohlcv, "silver.ohlcv"))

        # Duplicate key check
        results.append(check_duplicate_keys(ohlcv, ["symbol", "trade_date"], "silver.ohlcv"))

        write_dq_results(spark, results)

    except Exception as e:
        logger.error(f"DQ check run failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_dq_checks()

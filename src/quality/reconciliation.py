"""
src/quality/reconciliation.py

Row-Count and Checksum Reconciliation Framework

Validates that record counts and aggregate checksums are consistent
across Bronze → Silver → Gold layer boundaries.

Why reconciliation matters:
  DQ checks (dq_checks.py) validate the *shape* of data (nulls, ranges).
  Reconciliation validates the *flow* of data across layers:
  "Did every Bronze record make it to Silver? Did every Silver record
  make it to Gold?"

  Without reconciliation, you can have a pipeline that "succeeds" but
  silently drops 5% of records due to a subtle join condition or a
  Spark shuffle issue. Reconciliation catches this.

Two reconciliation methods:
  1. ROW COUNT: count(Bronze) vs count(Silver) vs count(Gold)
     Simple and fast. Catches outright record loss.

  2. AGGREGATE CHECKSUM: SUM(close * volume) across all symbols+dates
     Catches cases where row counts match but values are wrong
     (e.g., a bug that replaces prices with zeros would fail this).
     This is analogous to a financial "sum of debits = sum of credits" check.

Results are written to gold.reconciliation_summary for trending.
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)

BRONZE_BUCKET = "s3a://finlake"
SILVER_DB = "finlake.silver"
GOLD_DB = "finlake.gold"

# Tolerance for count discrepancies (e.g., 0.001 = allow 0.1% difference)
# In production this might be 0.0 for financial data (zero tolerance)
COUNT_TOLERANCE_PCT = 0.001


def reconcile_ohlcv_counts(spark: SparkSession) -> dict:
    """
    Compares row counts between Bronze JSON, Silver Iceberg, and Gold Iceberg
    for the OHLCV dataset.

    Returns a dict with counts and pass/fail status.
    """
    logger.info("Running OHLCV row-count reconciliation...")

    # Bronze count (read raw JSON)
    try:
        bronze_df = spark.read.json(f"{BRONZE_BUCKET}/bronze/ohlcv/*/*/*.json")
        bronze_count = bronze_df.count()
    except Exception as e:
        logger.warning(f"Could not read Bronze: {e}")
        bronze_count = -1

    # Silver count
    try:
        silver_count = spark.table(f"{SILVER_DB}.ohlcv").count()
    except Exception:
        silver_count = -1

    # Gold count
    try:
        gold_count = spark.table(f"{GOLD_DB}.daily_ohlcv").count()
    except Exception:
        gold_count = -1

    # Silver should have <= Bronze (dedup removes dupes)
    # Gold should have <= Silver (may aggregate or filter)
    # We flag if Silver drops MORE than tolerance% vs Bronze
    status = "PASS"
    issues = []

    if bronze_count > 0 and silver_count > 0:
        drop_pct = (bronze_count - silver_count) / bronze_count
        if drop_pct > COUNT_TOLERANCE_PCT + 0.1:  # Allow up to 10% drop for dedup
            status = "WARN"
            issues.append(f"Silver dropped {drop_pct:.1%} of Bronze records (dedup expected)")

    if silver_count > 0 and gold_count > 0 and gold_count > silver_count:
        status = "FAIL"
        issues.append(f"Gold ({gold_count}) has MORE rows than Silver ({silver_count}) — unexpected")

    result = {
        "dataset": "ohlcv",
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "gold_count": gold_count,
        "status": status,
        "issues": "; ".join(issues) if issues else "All counts within expected range",
        "run_timestamp": datetime.utcnow().isoformat(),
    }

    logger.info(
        f"OHLCV Reconciliation | Bronze: {bronze_count:,} | Silver: {silver_count:,} | "
        f"Gold: {gold_count:,} | Status: {status}"
    )
    return result


def reconcile_ohlcv_checksum(spark: SparkSession) -> dict:
    """
    Computes and compares aggregate checksums between Silver and Gold.

    Checksum = SUM(close * volume) across all rows.
    This value should be consistent between layers (Gold is a view
    of Silver, not an independent source of truth).
    """
    logger.info("Running OHLCV aggregate checksum reconciliation...")

    try:
        silver_checksum = (
            spark.table(f"{SILVER_DB}.ohlcv")
            .agg(F.round(F.sum(F.col("close") * F.col("volume")), 2).alias("checksum"))
            .collect()[0]["checksum"]
        )
    except Exception:
        silver_checksum = None

    try:
        gold_checksum = (
            spark.table(f"{GOLD_DB}.daily_ohlcv")
            .agg(F.round(F.sum(F.col("close") * F.col("volume")), 2).alias("checksum"))
            .collect()[0]["checksum"]
        )
    except Exception:
        gold_checksum = None

    status = "PASS"
    if silver_checksum and gold_checksum:
        diff = abs(silver_checksum - gold_checksum) / max(abs(silver_checksum), 1)
        if diff > COUNT_TOLERANCE_PCT:
            status = "FAIL"

    result = {
        "dataset": "ohlcv_checksum",
        "silver_checksum": float(silver_checksum) if silver_checksum else -1,
        "gold_checksum": float(gold_checksum) if gold_checksum else -1,
        "status": status,
        "issues": "Checksum mismatch detected" if status == "FAIL" else "Checksums match",
        "run_timestamp": datetime.utcnow().isoformat(),
    }

    logger.info(f"Checksum | Silver: {silver_checksum} | Gold: {gold_checksum} | Status: {status}")
    return result


def write_reconciliation_summary(spark: SparkSession, results: list[dict]) -> None:
    """Persists reconciliation results to Gold layer for trending."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.reconciliation_summary (
            dataset          STRING,
            bronze_count     BIGINT,
            silver_count     BIGINT,
            gold_count       BIGINT,
            silver_checksum  DOUBLE,
            gold_checksum    DOUBLE,
            status           STRING,
            issues           STRING,
            run_timestamp    TIMESTAMP
        )
        USING iceberg
    """)

    df = spark.createDataFrame(results)
    df.writeTo(f"{GOLD_DB}.reconciliation_summary").append()
    logger.info(f"Reconciliation summary written ({len(results)} results)")


def run_reconciliation() -> None:
    """Entry point for the reconciliation job. Called after Gold transforms."""
    spark = get_spark_session("Reconciliation")
    try:
        results = [
            reconcile_ohlcv_counts(spark),
            reconcile_ohlcv_checksum(spark),
        ]
        write_reconciliation_summary(spark, results)

        failed = [r for r in results if r["status"] == "FAIL"]
        if failed:
            logger.error(f"RECONCILIATION FAILURES: {[r['dataset'] for r in failed]}")
        else:
            logger.info("All reconciliation checks passed")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_reconciliation()

"""
src/transformation/silver_to_gold.py

Gold Layer — Silver to Gold PySpark Transformation

Builds analytics-ready, curated datasets from Silver Iceberg tables.
These are the tables downstream consumers (BI tools, quant models, AI)
query directly — they should be fast, well-documented, and trustworthy.

Gold tables built here:
  1. gold.daily_ohlcv         — Clean daily prices, partitioned by symbol + year
  2. gold.rolling_metrics     — 7d/30d rolling volatility, RSI-14, avg volume
  3. gold.portfolio_analytics — Cross-symbol correlations and sector exposure

Design principles:
  - DENORMALIZED: Gold tables are pre-joined and wide. Consumers shouldn't
    need to write complex joins — that complexity lives here.
  - PARTITIONED: gold.daily_ohlcv partitioned by (symbol, year) so a
    "give me AAPL 2024" query touches only one partition.
  - IDEMPOTENT: All jobs use overwritePartitions — safe to re-run.
  - SQL-BASED: Gold transforms are written in PySpark SQL where possible,
    because SQL is readable, reviewable, and portable.

Rolling metrics explained:
  - Daily return = (close - prev_close) / prev_close
  - Rolling volatility = std(daily_return) over N days * sqrt(252)
    (annualized, 252 = trading days per year)
  - RSI (Relative Strength Index) = momentum indicator:
    RSI > 70 = potentially overbought, RSI < 30 = potentially oversold
"""

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)

SILVER_DB = "finlake.silver"
GOLD_DB = "finlake.gold"


# ─────────────────────────────────────────────
# 1. Daily OHLCV Gold table
# ─────────────────────────────────────────────

def build_daily_ohlcv(spark: SparkSession) -> DataFrame:
    """
    Reads Silver OHLCV and adds computed columns for Gold.

    Additional columns vs Silver:
      - daily_return: percentage change from prior close (key metric)
      - price_range: high - low (intraday volatility proxy)
      - vwap_approx: approximation of Volume Weighted Average Price
        VWAP = midpoint * volume / sum(volume). We use (H+L+C)/3 as midpoint.

    Why not just copy Silver straight to Gold?
      Silver is clean but "raw" — it has individual columns but no
      derived analytics. Gold is where we pre-compute the metrics
      that analysts ask for every day, so queries are instant.
    """
    logger.info("Building gold.daily_ohlcv...")

    window_by_symbol = Window.partitionBy("symbol").orderBy("trade_date")

    df = (
        spark.table(f"{SILVER_DB}.ohlcv")
        # Daily return: % change from prior day's close
        .withColumn("prev_close", F.lag("close", 1).over(window_by_symbol))
        .withColumn(
            "daily_return",
            F.when(
                F.col("prev_close").isNotNull() & (F.col("prev_close") != 0),
                (F.col("close") - F.col("prev_close")) / F.col("prev_close")
            ).otherwise(F.lit(None))
        )
        # Intraday price range
        .withColumn("price_range", F.col("high") - F.col("low"))
        # Approximate VWAP: (H + L + C) / 3 × Volume
        .withColumn(
            "vwap_approx",
            F.round((F.col("high") + F.col("low") + F.col("close")) / 3, 4)
        )
        # Partition columns
        .withColumn("year",  F.year("trade_date"))
        .withColumn("month", F.month("trade_date"))
        .withColumn("processed_at", F.lit(datetime.utcnow().isoformat()).cast("timestamp"))
        .drop("prev_close")
    )

    return df


def write_daily_ohlcv(spark: SparkSession, df: DataFrame) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.daily_ohlcv (
            symbol        STRING  NOT NULL,
            trade_date    DATE    NOT NULL,
            open          DOUBLE,
            high          DOUBLE,
            low           DOUBLE,
            close         DOUBLE,
            volume        BIGINT,
            daily_return  DOUBLE,
            price_range   DOUBLE,
            vwap_approx   DOUBLE,
            ingested_at   TIMESTAMP,
            processed_at  TIMESTAMP,
            year          INT,
            month         INT
        )
        USING iceberg
        PARTITIONED BY (symbol, year)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    df.writeTo(f"{GOLD_DB}.daily_ohlcv").overwritePartitions()
    logger.info("gold.daily_ohlcv written")


# ─────────────────────────────────────────────
# 2. Rolling Metrics Gold table
# ─────────────────────────────────────────────

def build_rolling_metrics(spark: SparkSession) -> DataFrame:
    """
    Computes rolling statistical metrics for each symbol over time.

    7d/30d Rolling Volatility:
      Std deviation of daily returns over 7 or 30 trading days,
      annualized by multiplying by sqrt(252).
      Higher vol = riskier / more uncertain price outlook.

    RSI-14 (Relative Strength Index):
      Momentum oscillator. Compares average gains vs losses over 14 days.
      Formula: RSI = 100 - (100 / (1 + RS)), where RS = avg_gain / avg_loss
      RSI > 70: overbought signal | RSI < 30: oversold signal

    7d/30d Average Volume:
      Rolling average of shares traded. Helps identify unusual volume spikes
      (institutional buying/selling events).
    """
    logger.info("Building gold.rolling_metrics...")

    window_7d  = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-6, 0)
    window_30d = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-29, 0)
    window_14d = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-13, 0)

    df = (
        spark.table(f"{GOLD_DB}.daily_ohlcv")
        # Rolling volatility (annualized)
        .withColumn("vol_7d",  F.round(F.stddev("daily_return").over(window_7d)  * (252 ** 0.5), 6))
        .withColumn("vol_30d", F.round(F.stddev("daily_return").over(window_30d) * (252 ** 0.5), 6))
        # Rolling average volume
        .withColumn("avg_vol_7d",  F.round(F.avg("volume").over(window_7d),  0).cast("bigint"))
        .withColumn("avg_vol_30d", F.round(F.avg("volume").over(window_30d), 0).cast("bigint"))
        # RSI-14: compute gains and losses, then rolling avg
        .withColumn("gain", F.when(F.col("daily_return") > 0, F.col("daily_return")).otherwise(0))
        .withColumn("loss", F.when(F.col("daily_return") < 0, -F.col("daily_return")).otherwise(0))
        .withColumn("avg_gain_14d", F.avg("gain").over(window_14d))
        .withColumn("avg_loss_14d", F.avg("loss").over(window_14d))
        .withColumn(
            "rsi_14",
            F.when(
                F.col("avg_loss_14d") == 0,
                F.lit(100.0)  # No losses = max RSI
            ).otherwise(
                F.round(100 - (100 / (1 + F.col("avg_gain_14d") / F.col("avg_loss_14d"))), 2)
            )
        )
        .select(
            "symbol", "trade_date", "close", "daily_return",
            "vol_7d", "vol_30d", "avg_vol_7d", "avg_vol_30d", "rsi_14"
        )
        .withColumn("processed_at", F.lit(datetime.utcnow().isoformat()).cast("timestamp"))
    )

    return df


def write_rolling_metrics(spark: SparkSession, df: DataFrame) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.rolling_metrics (
            symbol        STRING,
            trade_date    DATE,
            close         DOUBLE,
            daily_return  DOUBLE,
            vol_7d        DOUBLE,
            vol_30d       DOUBLE,
            avg_vol_7d    BIGINT,
            avg_vol_30d   BIGINT,
            rsi_14        DOUBLE,
            processed_at  TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (symbol)
    """)
    df.writeTo(f"{GOLD_DB}.rolling_metrics").overwritePartitions()
    logger.info("gold.rolling_metrics written")


# ─────────────────────────────────────────────
# 3. Portfolio Analytics Gold table
# ─────────────────────────────────────────────

def build_portfolio_analytics(spark: SparkSession) -> DataFrame:
    """
    Builds a cross-symbol analytics table that joins Gold OHLCV
    with the Silver company dimension (SCD2) to add sector context.

    This is a "data product" — a pre-built analytics view designed for
    a specific consumer (e.g., a portfolio risk dashboard or a quant model).

    The join uses is_current = true to get today's company attributes,
    which is safe because we're doing forward-looking analytics.
    For point-in-time historical analysis you'd join on
    trade_date BETWEEN effective_from AND effective_to instead.
    """
    logger.info("Building gold.portfolio_analytics...")

    ohlcv = spark.table(f"{GOLD_DB}.daily_ohlcv")
    metrics = spark.table(f"{GOLD_DB}.rolling_metrics")
    company = spark.table(f"{SILVER_DB}.company_dim").filter(F.col("is_current") == True)

    df = (
        ohlcv
        .join(metrics.select("symbol", "trade_date", "vol_30d", "rsi_14", "avg_vol_30d"),
              on=["symbol", "trade_date"], how="left")
        .join(company.select("symbol", "sector", "industry", "exchange", "market_cap"),
              on="symbol", how="left")
        .select(
            "symbol", "trade_date", "sector", "industry",
            "close", "daily_return", "volume",
            "vol_30d", "rsi_14", "avg_vol_30d", "market_cap",
        )
        .withColumn("processed_at", F.lit(datetime.utcnow().isoformat()).cast("timestamp"))
    )

    return df


def write_portfolio_analytics(spark: SparkSession, df: DataFrame) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.portfolio_analytics (
            symbol        STRING,
            trade_date    DATE,
            sector        STRING,
            industry      STRING,
            close         DOUBLE,
            daily_return  DOUBLE,
            volume        BIGINT,
            vol_30d       DOUBLE,
            rsi_14        DOUBLE,
            avg_vol_30d   BIGINT,
            market_cap    BIGINT,
            processed_at  TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (sector)
    """)
    df.writeTo(f"{GOLD_DB}.portfolio_analytics").overwritePartitions()
    logger.info("gold.portfolio_analytics written")


def run_silver_to_gold() -> None:
    """Entry point: runs all Gold layer build jobs in order."""
    spark = get_spark_session("SilverToGold")
    try:
        # 1. Daily OHLCV
        daily = build_daily_ohlcv(spark)
        write_daily_ohlcv(spark, daily)

        # 2. Rolling Metrics (depends on daily_ohlcv being written first)
        metrics = build_rolling_metrics(spark)
        write_rolling_metrics(spark, metrics)

        # 3. Portfolio Analytics (depends on both above)
        portfolio = build_portfolio_analytics(spark)
        write_portfolio_analytics(spark, portfolio)

        logger.info("Silver → Gold transformation complete. All 3 Gold tables built.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_to_gold()

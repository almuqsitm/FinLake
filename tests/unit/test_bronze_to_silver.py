"""
tests/unit/test_bronze_to_silver.py

Unit tests for the Bronze to Silver OHLCV transformation.
"""

from datetime import datetime

import chispa
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.transformation.bronze_to_silver import clean_ohlcv


def test_clean_ohlcv_deduplication(spark: SparkSession):
    """
    Test that the clean_ohlcv function correctly deduplicates rows.
    If we ingest the exact same symbol and date twice, the logic should
    keep only the one with the most recent 'ingested_at' timestamp.
    """
    # 1. Setup raw fake data (two identical trade dates, different ingested_at)
    raw_data = [
        # Older ingestion (should be dropped)
        {"symbol": "AAPL", "date": "2024-01-15T00:00:00", "open": 150.0, "high": 155.0, "low": 149.0, "close": 154.0, "volume": 1000, "ingested_at": "2024-01-15T12:00:00"},
        # Newer ingestion (should be kept)
        {"symbol": "AAPL", "date": "2024-01-15T00:00:00", "open": 150.0, "high": 155.0, "low": 149.0, "close": 154.5, "volume": 1000, "ingested_at": "2024-01-15T18:00:00"},
        # Different date (should be kept)
        {"symbol": "AAPL", "date": "2024-01-16T00:00:00", "open": 155.0, "high": 160.0, "low": 154.0, "close": 159.0, "volume": 2000, "ingested_at": "2024-01-16T12:00:00"},
    ]
    raw_df = spark.createDataFrame(raw_data)

    # 2. Run the function we are testing
    clean_df = clean_ohlcv(raw_df)

    # 3. Assertions
    # We started with 3 rows, but one was a duplicate, so we should have 2 left
    assert clean_df.count() == 2

    # Check that the one we kept for 2024-01-15 is the newer one (close price was 154.5)
    kept_row = clean_df.filter(F.col("trade_date") == "2024-01-15").first()
    assert kept_row["close"] == 154.5


def test_clean_ohlcv_drops_invalid_prices(spark: SparkSession):
    """
    Test that the function drops rows with negative or zero close prices,
    as well as rows with missing symbols or dates.
    """
    raw_data = [
        # Valid row
        {"symbol": "MSFT", "date": "2024-01-15T00:00:00", "open": 400.0, "high": 405.0, "low": 399.0, "close": 404.0, "volume": 5000, "ingested_at": "2024-01-15T12:00:00"},
        # Invalid: Negative price (should drop)
        {"symbol": "MSFT", "date": "2024-01-16T00:00:00", "open": 400.0, "high": 405.0, "low": 399.0, "close": -10.0, "volume": 5000, "ingested_at": "2024-01-16T12:00:00"},
        # Invalid: Zero price (should drop)
        {"symbol": "MSFT", "date": "2024-01-17T00:00:00", "open": 400.0, "high": 405.0, "low": 399.0, "close": 0.0, "volume": 5000, "ingested_at": "2024-01-17T12:00:00"},
        # Invalid: Missing symbol (should drop)
        {"symbol": None,   "date": "2024-01-18T00:00:00", "open": 400.0, "high": 405.0, "low": 399.0, "close": 404.0, "volume": 5000, "ingested_at": "2024-01-18T12:00:00"},
    ]
    raw_df = spark.createDataFrame(raw_data)

    clean_df = clean_ohlcv(raw_df)

    # Only 1 out of the 4 rows should survive the data quality filters
    assert clean_df.count() == 1
    assert clean_df.first()["trade_date"].isoformat() == "2024-01-15"

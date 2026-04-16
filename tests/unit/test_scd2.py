"""
tests/unit/test_scd2.py

Unit tests for the Slowly Changing Dimension Type 2 logic.
"""

from datetime import date

from pyspark.sql import SparkSession

from src.transformation.scd2_company_dim import compute_record_hash


def test_scd2_hash_computation(spark: SparkSession):
    """
    Test that the record_hash logic properly assigns identical hashes
    to identical rows, and different hashes when attributes change.
    This is the core mechanic that allows SCD2 to detect changes.
    """
    data = [
        # Base record
        {"symbol": "GS", "short_name": "Goldman Sachs", "sector": "Financials", "industry": "Investment Banking", "exchange": "NYSE", "currency": "USD", "country": "USA", "market_cap": 1000},
        # Exact same attributes (market_cap isn't tracked for SCD2, so its change shouldn't affect hash)
        {"symbol": "GS", "short_name": "Goldman Sachs", "sector": "Financials", "industry": "Investment Banking", "exchange": "NYSE", "currency": "USD", "country": "USA", "market_cap": 2000},
        # Sector changed (hash MUST change)
        {"symbol": "GS", "short_name": "Goldman Sachs", "sector": "Technology", "industry": "Investment Banking", "exchange": "NYSE", "currency": "USD", "country": "USA", "market_cap": 1000},
    ]
    df = spark.createDataFrame(data)

    hashed_df = compute_record_hash(df).collect()

    hash_base = hashed_df[0]["record_hash"]
    hash_same = hashed_df[1]["record_hash"]
    hash_diff = hashed_df[2]["record_hash"]

    # The first two should match because SCD2_TRACKED_COLUMNS didn't change
    assert hash_base == hash_same

    # The third should be different because 'sector' changed
    assert hash_base != hash_diff

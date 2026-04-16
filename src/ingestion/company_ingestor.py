"""
src/ingestion/company_ingestor.py

Bronze Layer — Company Metadata Ingestion

Pulls company profile information (sector, industry, exchange, description,
employee count, etc.) from Yahoo Finance and writes raw JSON to MinIO.

Why is this separate from OHLCV ingestion?
  OHLCV changes every trading day — it's a time-series fact table.
  Company metadata changes infrequently (sector reclassifications,
  acquisitions, name changes). Separating them means:
  - Different ingestion cadence (OHLCV = daily, metadata = weekly)
  - Clean separation in the Bronze layer for easier debugging
  - The metadata feeds the SCD Type 2 dimension model in Silver,
    which tracks *historical changes* to company attributes over time

Storage pattern:
  bronze/company/{SYMBOL}/latest.json
  (latest snapshot — SCD2 logic in Silver handles the history)
"""

from datetime import datetime

import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential

from src.ingestion.batch_ingestor import TICKERS
from src.utils.logger import get_logger
from src.utils.minio_client import write_json_to_bronze

logger = get_logger(__name__)

# Fields we extract from yfinance's .info dict.
# yfinance returns ~150 fields — we pick the ones relevant
# to dimensional modeling and business context.
METADATA_FIELDS = [
    "symbol",
    "shortName",          # e.g. "Apple Inc."
    "longName",           # e.g. "Apple Inc."
    "sector",             # e.g. "Technology"
    "industry",           # e.g. "Consumer Electronics"
    "exchange",           # e.g. "NMS" (NASDAQ)
    "currency",           # e.g. "USD"
    "country",            # e.g. "United States"
    "state",              # e.g. "CA"
    "city",               # e.g. "Cupertino"
    "fullTimeEmployees",  # headcount snapshot
    "marketCap",          # market capitalisation at time of pull
    "longBusinessSummary",# business description (great for LLM/AI use cases)
    "website",
    "ipoYear",
]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def fetch_company_metadata(ticker: str) -> dict:
    """
    Fetches company profile data from Yahoo Finance for one ticker.

    yfinance's .info property returns a large dict of company attributes.
    We filter to just the fields in METADATA_FIELDS to keep Bronze clean
    and avoid storing junk data.

    Args:
        ticker: Stock symbol, e.g. 'GS' for Goldman Sachs

    Returns:
        Dict with company metadata fields + ingestion timestamp
    """
    logger.info(f"Fetching metadata for {ticker}")

    ticker_obj = yf.Ticker(ticker)
    info = ticker_obj.info  # API call happens here

    if not info or info.get("trailingPegRatio") is None and "shortName" not in info:
        logger.warning(f"Sparse metadata returned for {ticker} — may be rate-limited")

    # Extract only the fields we care about, default None if missing
    record = {field: info.get(field) for field in METADATA_FIELDS}

    # Ensure symbol is always set (yfinance sometimes omits it from .info)
    record["symbol"] = ticker

    # Ingestion metadata — when was this snapshot taken?
    # This is what the SCD2 logic uses to detect "has anything changed
    # since the last time we ingested this company?"
    record["snapshot_date"] = datetime.utcnow().strftime("%Y-%m-%d")
    record["ingested_at"] = datetime.utcnow().isoformat()

    return record


def ingest_company(ticker: str) -> None:
    """
    Fetches metadata for one ticker and writes it to Bronze.

    The file path is: bronze/company/{TICKER}/latest.json
    We overwrite 'latest' on each run — the Silver SCD2 layer
    is responsible for comparing this against the previous snapshot
    and deciding whether to create a new history record.

    Args:
        ticker: Stock symbol
    """
    record = fetch_company_metadata(ticker)
    s3_key = f"bronze/company/{ticker}/latest.json"
    write_json_to_bronze(record, s3_key)
    logger.info(f"{ticker}: company metadata written → {s3_key}")


def run_company_ingestion() -> None:
    """
    Main entry point. Loops through all tickers and ingests metadata.

    Called by the Airflow DAG as a separate task from OHLCV ingestion
    because they have different retry tolerances and schedules.
    """
    logger.info(f"Starting company metadata ingestion for {len(TICKERS)} tickers")

    failed = []
    for ticker in TICKERS:
        try:
            ingest_company(ticker)
        except Exception as e:
            logger.error(f"Failed to ingest metadata for {ticker}: {e}")
            failed.append(ticker)

    logger.info("Company metadata ingestion complete")
    if failed:
        logger.warning(f"Failed tickers: {failed}")


if __name__ == "__main__":
    run_company_ingestion()

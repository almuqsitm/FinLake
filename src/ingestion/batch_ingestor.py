"""
src/ingestion/batch_ingestor.py

Bronze Layer — Batch OHLCV Ingestion

Pulls historical daily Open/High/Low/Close/Volume data from Yahoo Finance
for a curated list of tickers and writes raw JSON to MinIO (Bronze layer).

Design principles applied here:
  1. BRONZE = RAW: We write data exactly as received. No type casting,
     no cleaning, no transformations. This is the audit trail.
     If Silver is wrong, we reprocess from Bronze.

  2. PARTITIONED STORAGE: Files are stored at:
       bronze/ohlcv/{SYMBOL}/{DATE}.json
     This means Spark can read just AAPL data by scanning only the
     bronze/ohlcv/AAPL/ prefix — massive performance gain at scale.

  3. IDEMPOTENT: Running this script twice doesn't corrupt data.
     MinIO's put_object overwrites the same key, so re-runs are safe.

  4. RETRY LOGIC: Yahoo Finance's API occasionally times out or rate-limits.
     The @retry decorator automatically retries failed calls up to 3 times
     with exponential backoff (wait 2s, then 4s, then 8s) before giving up.
"""

from datetime import datetime, timedelta

import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential

from src.utils.logger import get_logger
from src.utils.minio_client import write_json_to_bronze

logger = get_logger(__name__)

# ─────────────────────────────────────────────
# The tickers we track — a mix of sectors (tech, finance, healthcare,
# energy, consumer) to make the Gold layer analytics interesting.
# Goldman Sachs engineers care about cross-sector data coverage.
# ─────────────────────────────────────────────
TICKERS = [
    "AAPL",  # Apple — Technology
    "MSFT",  # Microsoft — Technology
    "JPM",   # JPMorgan Chase — Financials
    "GS",    # Goldman Sachs — Financials
    "JNJ",   # Johnson & Johnson — Healthcare
    "XOM",   # ExxonMobil — Energy
    "AMZN",  # Amazon — Consumer Discretionary
    "GOOGL", # Alphabet — Communication Services
    "BRK-B", # Berkshire Hathaway — Diversified
    "SPY",   # S&P 500 ETF — Benchmark index
]

# How far back to pull data (365 days gives us enough for 30d rolling metrics)
LOOKBACK_DAYS = 365


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def fetch_ohlcv(ticker: str, start: str, end: str) -> list[dict]:
    """
    Fetches daily OHLCV data from Yahoo Finance for one ticker.

    The @retry decorator means: if this function raises an exception,
    wait 2 seconds and try again, up to 3 total attempts. This handles
    transient network errors without crashing the whole pipeline.

    Args:
        ticker: Stock symbol, e.g. 'AAPL'
        start:  Start date string, e.g. '2023-01-01'
        end:    End date string, e.g. '2024-01-01'

    Returns:
        List of dicts, one per trading day:
        [{"date": "2024-01-15", "open": 185.01, "high": 186.00, ...}]
    """
    logger.info(f"Fetching OHLCV for {ticker} from {start} to {end}")

    ticker_obj = yf.Ticker(ticker)
    df = ticker_obj.history(start=start, end=end, auto_adjust=True)

    if df.empty:
        logger.warning(f"No data returned for {ticker}")
        return []

    # Convert DataFrame to list of records.
    # We reset the index because the date is stored as the DataFrame index
    # by default (yfinance quirk), and we want it as a regular column.
    df = df.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # Keep only the columns we care about
    cols = ["date", "open", "high", "low", "close", "volume"]
    df = df[[c for c in cols if c in df.columns]]

    # Add ingestion metadata — critical for debugging later:
    # "when was this record pulled?" lets you distinguish old Bronze
    # files from recently re-ingested ones
    records = df.to_dict(orient="records")
    for record in records:
        record["symbol"] = ticker
        record["ingested_at"] = datetime.utcnow().isoformat()

    return records


def ingest_ticker(ticker: str, start: str, end: str) -> int:
    """
    Fetches and writes Bronze JSON files for a single ticker.

    Each day's data is written to its own file:
      bronze/ohlcv/AAPL/2024-01-15.json

    This granularity lets us:
    - Re-ingest a single day without rewriting everything
    - Let Spark skip partitions it doesn't need (partition pruning)

    Returns:
        Number of records written
    """
    records = fetch_ohlcv(ticker, start, end)

    for record in records:
        # Extract just the date portion (yfinance returns datetime objects)
        date_str = str(record["date"])[:10]  # '2024-01-15T00:00:00' → '2024-01-15'
        s3_key = f"bronze/ohlcv/{ticker}/{date_str}.json"
        write_json_to_bronze(record, s3_key)

    logger.info(f"{ticker}: wrote {len(records)} daily records to Bronze")
    return len(records)


def run_batch_ingestion() -> None:
    """
    Main entry point. Loops through all tickers and ingests each one.

    In production this would be called by an Airflow task. For now
    it can be run directly: python -m src.ingestion.batch_ingestor
    """
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    logger.info(f"Starting batch ingestion: {start_date} to {end_date}")
    logger.info(f"Tickers: {TICKERS}")

    total_records = 0
    failed_tickers = []

    for ticker in TICKERS:
        try:
            count = ingest_ticker(ticker, start_date, end_date)
            total_records += count
        except Exception as e:
            logger.error(f"Failed to ingest {ticker}: {e}")
            failed_tickers.append(ticker)

    logger.info(f"Batch ingestion complete. Total records: {total_records:,}")
    if failed_tickers:
        logger.warning(f"Failed tickers: {failed_tickers}")


if __name__ == "__main__":
    run_batch_ingestion()

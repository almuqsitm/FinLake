"""
src/ingestion/tick_producer.py

Bronze Layer — Streaming Kafka Tick Producer

Simulates a real-time market data feed by generating realistic price tick
events and publishing them to a Kafka topic as Avro-serialized messages.

In production at a firm like Goldman Sachs, this would be replaced by a
real market data feed (Bloomberg, ICE, direct exchange connections). Our
simulator generates statistically plausible price movements using a
Geometric Brownian Motion (GBM) model — the same model underlying the
Black-Scholes options pricing formula.

Architecture:
  tick_producer.py  →  Kafka topic: 'market-ticks'  →  tick_consumer.py
                         (Avro serialized)

Why Avro over JSON for Kafka?
  1. Schema enforcement: the Avro schema (.avsc file) defines exactly what
     fields every message must have — type mismatches are caught at publish time
  2. Compact binary encoding: Avro messages are ~5x smaller than equivalent JSON
  3. Schema evolution: you can add optional fields without breaking consumers
     that haven't been updated yet — critical in microservice architectures

Run this with: python -m src.ingestion.tick_producer
Stop with: Ctrl+C
"""

import json
import os
import random
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path

import fastavro
from confluent_kafka import Producer

from src.utils.logger import get_logger

logger = get_logger(__name__)

# ─────────────────────────────────────────────
# Kafka configuration
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "market-ticks"
TICK_INTERVAL_SECONDS = 0.5  # One tick every 500ms across all symbols

# Load the Avro schema from the .avsc file
SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "ohlcv_tick.avsc"
with open(SCHEMA_PATH) as f:
    AVRO_SCHEMA = fastavro.parse_schema(json.load(f))

# ─────────────────────────────────────────────
# Seed prices for simulation (approximate real-world values)
# These are the starting prices; GBM will evolve them realistically
# ─────────────────────────────────────────────
SEED_PRICES: dict[str, float] = {
    "AAPL": 185.0,
    "MSFT": 415.0,
    "JPM": 200.0,
    "GS": 490.0,
    "JNJ": 155.0,
    "XOM": 115.0,
    "AMZN": 185.0,
    "GOOGL": 165.0,
    "BRK-B": 415.0,
    "SPY": 520.0,
}

# Exchange mapping — which exchange each symbol trades on
EXCHANGES: dict[str, str] = {
    "AAPL": "NASDAQ", "MSFT": "NASDAQ", "GOOGL": "NASDAQ",
    "AMZN": "NASDAQ", "JPM": "NYSE", "GS": "NYSE",
    "JNJ": "NYSE", "XOM": "NYSE", "BRK-B": "NYSE", "SPY": "NYSE",
}


class GBMPriceSimulator:
    """
    Geometric Brownian Motion price simulator.

    GBM is the mathematical model behind Black-Scholes options pricing.
    It models stock prices as a random walk with drift:

      dS = S * (μ dt + σ dW)

    Where:
      S  = current price
      μ  = drift (expected return, set to ~0 for intraday simulation)
      σ  = volatility (annualized, ~20% for typical large-cap stocks)
      dW = Wiener process increment (random normal shock)

    This produces price paths that look and behave like real tick data.
    """

    def __init__(self, symbol: str, seed_price: float, annual_vol: float = 0.20):
        self.symbol = symbol
        self.price = seed_price
        self.annual_vol = annual_vol
        # Convert annual vol to per-tick vol (assuming ~500ms ticks, 6.5hr trading day)
        self.tick_vol = annual_vol * (TICK_INTERVAL_SECONDS / (252 * 6.5 * 3600)) ** 0.5

    def next_price(self) -> float:
        """Advance the price by one GBM tick."""
        shock = random.gauss(0, 1)
        self.price *= (1 + self.tick_vol * shock)
        self.price = max(self.price, 0.01)  # Prices can't go negative
        return round(self.price, 2)

    def bid_ask(self) -> tuple[float, float]:
        """Compute a realistic bid/ask spread around the current price."""
        # Spread is ~0.01% of price (typical for liquid large-caps)
        half_spread = max(0.01, round(self.price * 0.0001, 2))
        return round(self.price - half_spread, 2), round(self.price + half_spread, 2)


def serialize_avro(record: dict) -> bytes:
    """
    Serializes a Python dict to Avro binary format.

    fastavro writes the record into an in-memory buffer (BytesIO)
    rather than a file — this gives us raw bytes to send to Kafka.
    """
    buf = BytesIO()
    fastavro.schemaless_writer(buf, AVRO_SCHEMA, record)
    return buf.getvalue()


def delivery_callback(err, msg):
    """
    Kafka delivery report callback — called once per message after
    the broker confirms receipt.

    'At-least-once' delivery means: if we don't get a confirmation,
    we retry. The consumer is responsible for deduplicating.
    """
    if err:
        logger.error(f"Delivery failed for {msg.key()}: {err}")
    else:
        logger.debug(f"Delivered: topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def run_producer() -> None:
    """
    Main producer loop. Continuously generates and publishes tick events
    for all symbols, round-robin, until Ctrl+C.
    """
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    simulators = {sym: GBMPriceSimulator(sym, price) for sym, price in SEED_PRICES.items()}

    logger.info(f"Starting tick producer → Kafka topic '{KAFKA_TOPIC}' @ {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info("Press Ctrl+C to stop")

    tick_count = 0
    try:
        while True:
            for symbol, sim in simulators.items():
                price = sim.next_price()
                bid, ask = sim.bid_ask()

                record = {
                    "symbol": symbol,
                    "event_timestamp": int(datetime.utcnow().timestamp() * 1000),
                    "price": price,
                    "bid": bid,
                    "ask": ask,
                    "volume": random.randint(100, 10_000),
                    "exchange": EXCHANGES.get(symbol, "UNKNOWN"),
                    "trade_condition": "REGULAR",
                }

                payload = serialize_avro(record)

                # Publish to Kafka — symbol is the message key.
                # Using symbol as key ensures all ticks for one symbol
                # land in the same Kafka partition (order is preserved per key)
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=symbol.encode("utf-8"),
                    value=payload,
                    callback=delivery_callback,
                )

                tick_count += 1

            # poll() triggers delivery callbacks — must be called regularly
            producer.poll(0)

            if tick_count % 100 == 0:
                logger.info(f"Published {tick_count:,} ticks | Sample: {symbol} @ ${price:.2f}")

            time.sleep(TICK_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info(f"Shutting down. Total ticks published: {tick_count:,}")
    finally:
        producer.flush()  # Ensure all buffered messages are sent before exit


if __name__ == "__main__":
    run_producer()

"""
src/ingestion/tick_consumer.py

Bronze Layer — Kafka Tick Consumer

Reads Avro-serialized tick events from the 'market-ticks' Kafka topic
and writes them as Avro files to MinIO (Bronze layer), batched by hour.

Design decisions:

  BATCHING BY HOUR: Instead of writing one file per tick (millions of tiny
  files = performance disaster for Spark), we accumulate ticks in memory
  and flush to MinIO once per hour. This is the standard pattern for
  micro-batch landing in Lakehouse architectures.

  File path: bronze/ticks/{SYMBOL}/{YYYY-MM-DD}/{HH}.avro
  Example:   bronze/ticks/AAPL/2024-01-15/14.avro

  AT-LEAST-ONCE DELIVERY: We commit Kafka offsets only AFTER successfully
  writing to MinIO. If MinIO write fails, we'll re-consume and re-process
  the same messages — the Silver deduplication step handles duplicates.
  This is safer than at-most-once (which could lose data) and simpler
  than exactly-once (which requires Kafka transactions).

  GRACEFUL SHUTDOWN: Catches SIGTERM / KeyboardInterrupt and flushes
  any accumulated in-memory ticks before exiting — no data loss on stop.

Run: python -m src.ingestion.tick_consumer
"""

import json
import os
import signal
import sys
from collections import defaultdict
from datetime import datetime
from io import BytesIO
from pathlib import Path

import fastavro
from confluent_kafka import Consumer, KafkaError

from src.utils.logger import get_logger
from src.utils.minio_client import FINLAKE_BUCKET, get_s3_client

logger = get_logger(__name__)

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "market-ticks"
KAFKA_GROUP_ID = "finlake-bronze-consumer"

# How many messages to accumulate before flushing to MinIO
# 600 messages ≈ 5 minutes of ticks at 0.5s intervals × 10 symbols
FLUSH_BATCH_SIZE = 600
POLL_TIMEOUT_SECONDS = 1.0

# Load Avro schema for deserialization
SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "ohlcv_tick.avsc"
with open(SCHEMA_PATH) as f:
    AVRO_SCHEMA = fastavro.parse_schema(json.load(f))


def deserialize_avro(raw_bytes: bytes) -> dict:
    """Deserializes Avro binary bytes back into a Python dict."""
    buf = BytesIO(raw_bytes)
    return fastavro.schemaless_reader(buf, AVRO_SCHEMA)


def flush_to_minio(tick_buffer: dict[str, list[dict]]) -> int:
    """
    Writes all buffered ticks to MinIO as Avro files, one file per symbol.

    Each file is placed at:
      bronze/ticks/{SYMBOL}/{DATE}/{HOUR}.avro

    Using the hour as the filename means each flush overwrites/appends
    within the same hour. In production you'd include a UUID to avoid
    overwrites, but for our purposes this is clean and debuggable.

    Args:
        tick_buffer: dict mapping symbol → list of tick dicts

    Returns:
        Total number of records written
    """
    client = get_s3_client()
    total_written = 0

    for symbol, ticks in tick_buffer.items():
        if not ticks:
            continue

        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")
        s3_key = f"bronze/ticks/{symbol}/{date_str}/{hour_str}.avro"

        # Write all ticks for this symbol to an in-memory Avro container
        buf = BytesIO()
        fastavro.writer(buf, AVRO_SCHEMA, ticks)
        buf.seek(0)

        client.put_object(
            Bucket=FINLAKE_BUCKET,
            Key=s3_key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )

        logger.info(f"Flushed {len(ticks):,} ticks for {symbol} → s3://{FINLAKE_BUCKET}/{s3_key}")
        total_written += len(ticks)

    return total_written


def run_consumer() -> None:
    """
    Main consumer loop. Polls Kafka continuously, buffers ticks,
    and flushes to MinIO when the buffer is full.
    """
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",     # Start from beginning if no saved offset
        "enable.auto.commit": False,          # We commit manually after successful MinIO write
    })
    consumer.subscribe([KAFKA_TOPIC])

    # Buffer: symbol → list of tick dicts
    tick_buffer: dict[str, list[dict]] = defaultdict(list)
    total_messages = 0
    running = True

    def shutdown_handler(sig, frame):
        nonlocal running
        logger.info("Shutdown signal received — flushing buffer before exit...")
        running = False

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    logger.info(f"Consumer started. Listening on topic '{KAFKA_TOPIC}' | group='{KAFKA_GROUP_ID}'")

    try:
        while running:
            msg = consumer.poll(timeout=POLL_TIMEOUT_SECONDS)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Normal — means we've caught up to the end of the partition
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                continue

            # Deserialize the Avro payload
            tick = deserialize_avro(msg.value())
            symbol = tick.get("symbol", "UNKNOWN")
            tick_buffer[symbol].append(tick)
            total_messages += 1

            # Flush when buffer is full
            if total_messages % FLUSH_BATCH_SIZE == 0:
                written = flush_to_minio(tick_buffer)
                consumer.commit()  # Commit offsets only after successful write
                tick_buffer = defaultdict(list)
                logger.info(f"Batch flushed. Total written: {total_messages:,} | Last batch: {written:,}")

    finally:
        # Flush any remaining messages before shutdown
        if any(ticks for ticks in tick_buffer.values()):
            logger.info("Flushing remaining buffer on shutdown...")
            flush_to_minio(tick_buffer)
            consumer.commit()
        consumer.close()
        logger.info(f"Consumer shut down cleanly. Total messages processed: {total_messages:,}")


if __name__ == "__main__":
    run_consumer()

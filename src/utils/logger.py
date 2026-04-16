"""
src/utils/logger.py

Provides a standardized logger for all FinLake modules.

Why structured logging?
  In production pipelines, logs are ingested into log aggregation tools
  (Splunk, Datadog, etc.). A consistent format makes it trivial to filter
  by module, severity, or timestamp across thousands of log lines.
"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger configured with a consistent format.

    Args:
        name: typically __name__ from the calling module,
              e.g. 'src.ingestion.batch_ingestor'
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger

"""
tests/conftest.py

Shared pytest fixtures.

Pytest fixtures are setup functions that run before your tests.
This file is automatically discovered by pytest.
We use it to create a single, shared SparkSession that all tests
can use. Creating a Spark session is slow (takes a few seconds),
so sharing one across all tests speeds up the test suite dramatically.
"""

import pytest
from pyspark.sql import SparkSession

from src.utils.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Creates a local Spark session for testing.
    'scope="session"' means it is created exactly once when you run pytest,
    and torn down after the last test finishes.
    """
    # use local=True so it doesn't try to connect to a real cluster or MinIO
    session = get_spark_session(app_name="pytest-finlake", local=True)
    yield session
    session.stop()

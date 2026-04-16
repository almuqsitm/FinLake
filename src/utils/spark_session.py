"""
src/utils/spark_session.py

Shared SparkSession factory for all FinLake PySpark jobs.

Why a factory function instead of creating SparkSession inline?
  Every PySpark job in this project imports get_spark_session().
  Centralising it here means:
  - One place to change config (S3 endpoint, Iceberg version, memory settings)
  - Tests can override it to get a lightweight local session
  - Spark deduplicates: if a session already exists, getOrCreate() returns it

Apache Iceberg integration:
  SparkSession needs two things to work with Iceberg tables:
  1. The Iceberg Spark runtime JAR (specified in spark.jars.packages)
  2. A catalog configuration (tells Spark where Iceberg metadata lives)

  We use the 'hadoop' catalog backed by MinIO (S3-compatible). In
  production this would be a Glue catalog, Hive metastore, or Nessie.

S3/MinIO integration:
  Spark reads/writes MinIO using the hadoop-aws connector. We configure
  it with the MinIO endpoint URL and credentials so Spark can resolve
  paths like s3a://finlake/bronze/ohlcv/...
"""

import os

from pyspark.sql import SparkSession

# ─────────────────────────────────────────────
# Versions — keep these in sync with requirements-dev.txt
# ─────────────────────────────────────────────
ICEBERG_VERSION = "1.5.2"
HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_VERSION = "1.12.367"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
WAREHOUSE_BUCKET = os.getenv("WAREHOUSE_BUCKET", "s3a://finlake-warehouse/")


def get_spark_session(app_name: str = "FinLake", local: bool = False) -> SparkSession:
    """
    Creates or retrieves an existing SparkSession configured for:
      - Apache Iceberg table format
      - MinIO (S3-compatible) storage backend
      - Reasonable local-mode resource settings

    Args:
        app_name: Name shown in the Spark UI (e.g. "BronzeToSilver")
        local:    If True, forces local[*] mode (used in unit tests)

    Returns:
        Configured SparkSession
    """
    # Build the JAR package string — Spark downloads these from Maven
    # on first run; they cache in ~/.ivy2 afterwards
    packages = ",".join([
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
        f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
        f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}",
    ])

    master = "local[*]" if local else os.getenv("SPARK_MASTER", "local[*]")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # ── Iceberg catalog configuration ──────────────────────────
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.finlake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.finlake.type", "hadoop")
        .config("spark.sql.catalog.finlake.warehouse", WAREHOUSE_BUCKET)
        # ── S3 / MinIO configuration ────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ── Resource settings (tuned for 16GB laptop) ───────────────
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "20")  # Default 200 is too many for local
        # ── Package downloads ───────────────────────────────────────
        .config("spark.jars.packages", packages)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")  # Suppress verbose Spark INFO logs
    return spark

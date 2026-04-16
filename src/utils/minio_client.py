"""
src/utils/minio_client.py

Provides a reusable MinIO (S3-compatible) client for reading and writing
data files in the Bronze layer.

Why MinIO instead of real S3?
  MinIO is API-identical to AWS S3. The boto3 code here works against
  real S3 with zero changes — you just swap the endpoint_url and
  credentials via environment variables. This is exactly how firms
  run local dev environments that mirror cloud production.
"""

import json
import os

import boto3
from botocore.client import Config

from src.utils.logger import get_logger

logger = get_logger(__name__)

# ─────────────────────────────────────────────
# Configuration (reads from env vars or falls back to local defaults)
# In production these would come from a secrets manager (Vault, AWS SSM)
# ─────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
FINLAKE_BUCKET = os.getenv("FINLAKE_BUCKET", "finlake")


def get_s3_client():
    """
    Returns a boto3 S3 client configured to talk to local MinIO.

    boto3 is Amazon's official AWS SDK for Python. Because MinIO
    implements the S3 API spec, boto3 works against it out of the box
    — you just point it at the MinIO endpoint instead of AWS.
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),  # MinIO requires v4 signing
        region_name="us-east-1",  # Required by boto3 even for MinIO
    )


def write_json_to_bronze(data: dict | list, s3_key: str) -> None:
    """
    Serializes data to JSON and writes it to the Bronze bucket.

    Args:
        data:   The Python dict or list to serialize
        s3_key: The S3 object key (acts like a file path),
                e.g. 'bronze/ohlcv/AAPL/2024-01-15.json'
    """
    client = get_s3_client()
    body = json.dumps(data, indent=2, default=str).encode("utf-8")

    client.put_object(
        Bucket=FINLAKE_BUCKET,
        Key=s3_key,
        Body=body,
        ContentType="application/json",
    )
    logger.info(f"Written to s3://{FINLAKE_BUCKET}/{s3_key} ({len(body):,} bytes)")


def list_bronze_keys(prefix: str) -> list[str]:
    """
    Lists all object keys under a given Bronze prefix.

    Args:
        prefix: e.g. 'bronze/ohlcv/AAPL/'
    Returns:
        List of S3 object key strings
    """
    client = get_s3_client()
    response = client.list_objects_v2(Bucket=FINLAKE_BUCKET, Prefix=prefix)
    keys = [obj["Key"] for obj in response.get("Contents", [])]
    logger.info(f"Found {len(keys)} objects under prefix '{prefix}'")
    return keys


def read_json_from_bronze(s3_key: str) -> dict | list:
    """
    Reads a JSON file from Bronze and deserializes it.

    Args:
        s3_key: The S3 object key to read
    Returns:
        Parsed Python object (dict or list)
    """
    client = get_s3_client()
    response = client.get_object(Bucket=FINLAKE_BUCKET, Key=s3_key)
    return json.loads(response["Body"].read().decode("utf-8"))

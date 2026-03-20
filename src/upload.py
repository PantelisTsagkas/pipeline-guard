"""
upload.py — Upload transformed weather data to AWS S3.

Stores both the raw JSON and the processed CSV under a structured
time-partitioned key:

    s3://<bucket>/weather/raw/<city>/YYYY/MM/DD/<timestamp>Z.json
    s3://<bucket>/weather/processed/<city>/YYYY/MM/DD/<timestamp>Z.csv
"""

import io
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ── S3 Key Builder ─────────────────────────────────────────────────────────────

def build_s3_key(city: str, timestamp: datetime, file_type: str) -> str:
    """
    Build a time-partitioned S3 key for a weather record.

    Pattern:
        weather/{raw|processed}/<city>/YYYY/MM/DD/<HHMMSSz>.<ext>

    Args:
        city:       City name (will be lowercased and spaces replaced with _)
        timestamp:  UTC datetime of the observation
        file_type:  "raw" (→ .json) or "processed" (→ .csv)

    Returns:
        S3 key string, e.g. "weather/raw/athens/2026/03/20/120000Z.json"
    """
    if file_type not in ("raw", "processed"):
        raise ValueError(f"file_type must be 'raw' or 'processed', got: {file_type!r}")

    ext = "json" if file_type == "raw" else "csv"
    city_slug = city.lower().replace(" ", "_")
    time_str = timestamp.strftime("%H%M%SZ")
    date_path = timestamp.strftime("%Y/%m/%d")

    return f"weather/{file_type}/{city_slug}/{date_path}/{time_str}.{ext}"


# ── Upload Result ──────────────────────────────────────────────────────────────

@dataclass
class UploadResult:
    success: bool
    bucket: str
    key: str
    s3_uri: str
    error: str = None

    def __repr__(self):
        status = "OK" if self.success else f"FAILED({self.error})"
        return f"<UploadResult {status} → {self.s3_uri}>"


# ── Core Upload Functions ──────────────────────────────────────────────────────

def _get_s3_client():
    """Create a boto3 S3 client from environment variables.

    Values are stripped of surrounding whitespace so that a secret saved
    with a trailing newline (a common copy-paste mistake in GitHub Actions)
    does not produce a malformed endpoint URL.
    """
    region = (os.getenv("AWS_REGION", "eu-north-1") or "eu-north-1").strip()
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=(os.getenv("AWS_ACCESS_KEY_ID") or "").strip() or None,
        aws_secret_access_key=(os.getenv("AWS_SECRET_ACCESS_KEY") or "").strip() or None,
    )


def upload_raw(raw: dict, city: str, timestamp: datetime, bucket: str, s3_client=None) -> UploadResult:
    """
    Upload raw JSON weather response to S3.

    Args:
        raw:        Raw API response dictionary
        city:       City name
        timestamp:  UTC observation datetime
        bucket:     S3 bucket name
        s3_client:  Optional pre-built boto3 S3 client (for testing/injection)

    Returns:
        UploadResult
    """
    key = build_s3_key(city, timestamp, "raw")
    s3_uri = f"s3://{bucket}/{key}"

    try:
        client = s3_client or _get_s3_client()
        body = json.dumps(raw, indent=2).encode("utf-8")
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            Metadata={
                "city": city,
                "observed_at": timestamp.isoformat(),
                "pipeline": "pipeline-guard",
            },
        )
        logger.info(f"✅ Raw JSON uploaded → {s3_uri}")
        return UploadResult(success=True, bucket=bucket, key=key, s3_uri=s3_uri)

    except (BotoCoreError, ClientError, ValueError) as e:
        logger.error(f"❌ Failed to upload raw JSON: {e}")
        return UploadResult(success=False, bucket=bucket, key=key, s3_uri=s3_uri, error=str(e))


def upload_processed(df: pd.DataFrame, city: str, timestamp: datetime, bucket: str, s3_client=None) -> UploadResult:
    """
    Upload processed (validated) DataFrame as CSV to S3.

    Args:
        df:         Validated weather DataFrame from validate.run()
        city:       City name
        timestamp:  UTC observation datetime
        bucket:     S3 bucket name
        s3_client:  Optional pre-built boto3 S3 client (for testing/injection)

    Returns:
        UploadResult
    """
    key = build_s3_key(city, timestamp, "processed")
    s3_uri = f"s3://{bucket}/{key}"

    try:
        client = s3_client or _get_s3_client()
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        body = buffer.getvalue().encode("utf-8")

        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="text/csv",
            Metadata={
                "city": city,
                "observed_at": timestamp.isoformat(),
                "pipeline": "pipeline-guard",
                "rows": str(len(df)),
            },
        )
        logger.info(f"✅ Processed CSV uploaded → {s3_uri}")
        return UploadResult(success=True, bucket=bucket, key=key, s3_uri=s3_uri)

    except (BotoCoreError, ClientError, ValueError) as e:
        logger.error(f"❌ Failed to upload processed CSV: {e}")
        return UploadResult(success=False, bucket=bucket, key=key, s3_uri=s3_uri, error=str(e))


# ── Main Entry Point ───────────────────────────────────────────────────────────

def run(raw: dict, df: pd.DataFrame, bucket: str = None, s3_client=None) -> dict:
    """
    Upload both raw JSON and processed CSV to S3.

    Args:
        raw:        Raw API response dictionary
        df:         Validated weather DataFrame
        bucket:     S3 bucket name (falls back to S3_BUCKET_NAME env var)
        s3_client:  Optional injected boto3 client

    Returns:
        Dict with keys "raw" and "processed", each an UploadResult

    Raises:
        EnvironmentError: If bucket name is not provided or set in env
        RuntimeError: If either upload fails
    """
    bucket = (bucket or os.getenv("S3_BUCKET_NAME") or "").strip() or None
    if not bucket:
        raise EnvironmentError("S3_BUCKET_NAME is not set in environment or .env file")

    # Parse observation timestamp from the validated DataFrame
    observed_at_str = df["observed_at_utc"].iloc[0]
    try:
        timestamp = datetime.fromisoformat(observed_at_str)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
    except ValueError:
        timestamp = datetime.now(timezone.utc)

    city = df["city"].iloc[0]

    raw_result       = upload_raw(raw, city, timestamp, bucket, s3_client)
    processed_result = upload_processed(df, city, timestamp, bucket, s3_client)

    results = {"raw": raw_result, "processed": processed_result}

    failed = [k for k, v in results.items() if not v.success]
    if failed:
        raise RuntimeError(
            f"S3 upload failed for: {', '.join(failed)}. "
            f"Check logs for details."
        )

    return results


if __name__ == "__main__":
    from src import ingest, transform, validate

    raw = ingest.run()
    df  = transform.run(raw)
    df  = validate.run(df)
    results = run(raw, df)

    for name, result in results.items():
        print(f"{name}: {result}")

"""
tests/test_upload.py — Unit tests for upload.py

All S3 calls are mocked with unittest.mock — no real AWS credentials needed.

Tests cover:
- S3 key structure and partitioning
- Successful raw JSON upload
- Successful processed CSV upload
- run() happy path (both uploads succeed)
- run() raises RuntimeError when an upload fails
- Correct Content-Type headers
- Correct S3 metadata
- Bucket env var fallback
- Missing bucket raises EnvironmentError
- Timestamp parsing from DataFrame
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from src.upload import (
    build_s3_key,
    upload_raw,
    upload_processed,
    run,
    UploadResult,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

FIXED_TS = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
BUCKET    = "pipeline-guard-data"
CITY      = "Athens"


@pytest.fixture
def mock_s3():
    """A MagicMock that stands in for a boto3 S3 client."""
    client = MagicMock()
    client.put_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    return client


@pytest.fixture
def mock_raw():
    return {
        "name": "Athens",
        "sys": {"country": "GR", "sunrise": 1719980000, "sunset": 1720030000},
        "coord": {"lat": 37.97, "lon": 23.72},
        "main": {"temp": 28.4, "feels_like": 27.9, "temp_min": 25.0, "temp_max": 30.1,
                 "pressure": 1013, "humidity": 45},
        "wind": {"speed": 4.5, "deg": 180},
        "clouds": {"all": 0},
        "weather": [{"main": "Clear", "description": "clear sky"}],
        "visibility": 10000,
        "dt": int(FIXED_TS.timestamp()),
    }


@pytest.fixture
def valid_df():
    return pd.DataFrame([{
        "city":             CITY,
        "country":          "GR",
        "latitude":         37.97,
        "longitude":        23.72,
        "temp_c":           28.4,
        "feels_like_c":     27.9,
        "temp_min_c":       25.0,
        "temp_max_c":       30.1,
        "humidity_pct":     45,
        "pressure_hpa":     1013,
        "visibility_m":     pd.array([10000], dtype="Int64")[0],
        "wind_speed_ms":    4.5,
        "wind_deg":         pd.array([180], dtype="Int64")[0],
        "weather_main":     "Clear",
        "weather_desc":     "clear sky",
        "cloud_pct":        0,
        "sunrise_utc":      "2026-03-20T04:53:20+00:00",
        "sunset_utc":       "2026-03-20T19:00:00+00:00",
        "observed_at_utc":  FIXED_TS.isoformat(),
        "ingested_at_utc":  FIXED_TS.isoformat(),
    }]).astype({"humidity_pct": int, "pressure_hpa": int, "cloud_pct": int})


# ── build_s3_key ──────────────────────────────────────────────────────────────

class TestBuildS3Key:

    def test_raw_key_structure(self):
        key = build_s3_key(CITY, FIXED_TS, "raw")
        assert key == "weather/raw/athens/2026/03/20/120000Z.json"

    def test_processed_key_structure(self):
        key = build_s3_key(CITY, FIXED_TS, "processed")
        assert key == "weather/processed/athens/2026/03/20/120000Z.csv"

    def test_city_slug_spaces(self):
        key = build_s3_key("New York", FIXED_TS, "raw")
        assert "new_york" in key

    def test_city_slug_lowercase(self):
        key = build_s3_key("LONDON", FIXED_TS, "raw")
        assert "london" in key

    def test_date_partition_year(self):
        key = build_s3_key(CITY, FIXED_TS, "raw")
        assert "/2026/" in key

    def test_date_partition_month(self):
        key = build_s3_key(CITY, FIXED_TS, "raw")
        assert "/03/" in key

    def test_date_partition_day(self):
        key = build_s3_key(CITY, FIXED_TS, "raw")
        assert "/20/" in key

    def test_invalid_file_type_raises(self):
        with pytest.raises(ValueError, match="file_type must be"):
            build_s3_key(CITY, FIXED_TS, "archive")

    def test_raw_extension_is_json(self):
        key = build_s3_key(CITY, FIXED_TS, "raw")
        assert key.endswith(".json")

    def test_processed_extension_is_csv(self):
        key = build_s3_key(CITY, FIXED_TS, "processed")
        assert key.endswith(".csv")


# ── upload_raw ────────────────────────────────────────────────────────────────

class TestUploadRaw:

    def test_returns_upload_result(self, mock_raw, mock_s3):
        result = upload_raw(mock_raw, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        assert isinstance(result, UploadResult)

    def test_success_is_true(self, mock_raw, mock_s3):
        result = upload_raw(mock_raw, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        assert result.success is True

    def test_put_object_called_once(self, mock_raw, mock_s3):
        upload_raw(mock_raw, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        mock_s3.put_object.assert_called_once()

    def test_correct_bucket(self, mock_raw, mock_s3):
        upload_raw(mock_raw, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        kwargs = mock_s3.put_object.call_args.kwargs
        assert kwargs["Bucket"] == BUCKET

    def test_correct_content_type(self, mock_raw, mock_s3):
        upload_raw(mock_raw, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        kwargs = mock_s3.put_object.call_args.kwargs
        assert kwargs["ContentType"] == "application/json"

    def test_body_is_valid_json(self, mock_raw, mock_s3):
        upload_raw(mock_raw, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        kwargs = mock_s3.put_object.call_args.kwargs
        parsed = json.loads(kwargs["Body"].decode("utf-8"))
        assert parsed["name"] == "Athens"

    def test_s3_uri_in_result(self, mock_raw, mock_s3):
        result = upload_raw(mock_raw, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        assert result.s3_uri.startswith(f"s3://{BUCKET}/")

    def test_metadata_contains_city(self, mock_raw, mock_s3):
        upload_raw(mock_raw, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        kwargs = mock_s3.put_object.call_args.kwargs
        assert kwargs["Metadata"]["city"] == CITY

    def test_s3_error_returns_failed_result(self, mock_raw, mock_s3):
        from botocore.exceptions import ClientError
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "Bucket not found"}}, "PutObject"
        )
        result = upload_raw(mock_raw, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        assert result.success is False
        assert result.error is not None


# ── upload_processed ──────────────────────────────────────────────────────────

class TestUploadProcessed:

    def test_returns_upload_result(self, valid_df, mock_s3):
        result = upload_processed(valid_df, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        assert isinstance(result, UploadResult)

    def test_success_is_true(self, valid_df, mock_s3):
        result = upload_processed(valid_df, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        assert result.success is True

    def test_correct_content_type(self, valid_df, mock_s3):
        upload_processed(valid_df, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        kwargs = mock_s3.put_object.call_args.kwargs
        assert kwargs["ContentType"] == "text/csv"

    def test_body_is_csv_with_header(self, valid_df, mock_s3):
        upload_processed(valid_df, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        kwargs = mock_s3.put_object.call_args.kwargs
        csv_text = kwargs["Body"].decode("utf-8")
        assert "city" in csv_text.split("\n")[0]
        assert "Athens" in csv_text

    def test_key_ends_with_csv(self, valid_df, mock_s3):
        result = upload_processed(valid_df, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        assert result.key.endswith(".csv")

    def test_metadata_row_count(self, valid_df, mock_s3):
        upload_processed(valid_df, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        kwargs = mock_s3.put_object.call_args.kwargs
        assert kwargs["Metadata"]["rows"] == "1"

    def test_s3_error_returns_failed_result(self, valid_df, mock_s3):
        from botocore.exceptions import ClientError
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "PutObject"
        )
        result = upload_processed(valid_df, CITY, FIXED_TS, BUCKET, s3_client=mock_s3)
        assert result.success is False


# ── run() ─────────────────────────────────────────────────────────────────────

class TestRun:

    def test_run_returns_dict(self, mock_raw, valid_df, mock_s3):
        results = run(mock_raw, valid_df, bucket=BUCKET, s3_client=mock_s3)
        assert isinstance(results, dict)

    def test_run_has_raw_and_processed_keys(self, mock_raw, valid_df, mock_s3):
        results = run(mock_raw, valid_df, bucket=BUCKET, s3_client=mock_s3)
        assert "raw" in results
        assert "processed" in results

    def test_run_both_succeed(self, mock_raw, valid_df, mock_s3):
        results = run(mock_raw, valid_df, bucket=BUCKET, s3_client=mock_s3)
        assert results["raw"].success is True
        assert results["processed"].success is True

    def test_run_calls_put_object_twice(self, mock_raw, valid_df, mock_s3):
        run(mock_raw, valid_df, bucket=BUCKET, s3_client=mock_s3)
        assert mock_s3.put_object.call_count == 2

    def test_run_raises_on_upload_failure(self, mock_raw, valid_df, mock_s3):
        from botocore.exceptions import ClientError
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "Bucket not found"}}, "PutObject"
        )
        with pytest.raises(RuntimeError, match="S3 upload failed"):
            run(mock_raw, valid_df, bucket=BUCKET, s3_client=mock_s3)

    def test_run_missing_bucket_raises_env_error(self, mock_raw, valid_df, mock_s3):
        with patch.dict("os.environ", {}, clear=True):
            # Make sure S3_BUCKET_NAME is not set
            import os
            os.environ.pop("S3_BUCKET_NAME", None)
            with pytest.raises(EnvironmentError, match="S3_BUCKET_NAME"):
                run(mock_raw, valid_df, bucket=None, s3_client=mock_s3)

    def test_run_uses_env_bucket_as_fallback(self, mock_raw, valid_df, mock_s3):
        with patch.dict("os.environ", {"S3_BUCKET_NAME": "env-bucket"}):
            results = run(mock_raw, valid_df, bucket=None, s3_client=mock_s3)
            assert results["raw"].bucket == "env-bucket"

    def test_run_parses_city_from_dataframe(self, mock_raw, valid_df, mock_s3):
        results = run(mock_raw, valid_df, bucket=BUCKET, s3_client=mock_s3)
        assert "athens" in results["raw"].key


# ── UploadResult repr ─────────────────────────────────────────────────────────

class TestUploadResultRepr:

    def test_repr_success(self):
        r = UploadResult(success=True, bucket=BUCKET, key="some/key.json",
                         s3_uri=f"s3://{BUCKET}/some/key.json")
        assert "OK" in repr(r)

    def test_repr_failure(self):
        r = UploadResult(success=False, bucket=BUCKET, key="some/key.json",
                         s3_uri=f"s3://{BUCKET}/some/key.json", error="AccessDenied")
        assert "FAILED" in repr(r)
        assert "AccessDenied" in repr(r)

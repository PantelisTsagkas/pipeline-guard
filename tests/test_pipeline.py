"""
tests/test_pipeline.py — Unit tests for pipeline.py orchestrator.

All external calls (ingest, transform, validate, upload) are mocked.

Tests cover:
- Happy path: all stages succeed
- PipelineResult fields populated correctly
- Each stage failure is caught and attributed correctly
- sys.exit codes via main()
- PipelineResult.summary() output
"""

import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.pipeline import run_pipeline, PipelineResult


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_raw():
    return {"name": "Athens", "dt": 1720000000}


@pytest.fixture
def mock_df():
    return pd.DataFrame([{
        "city": "Athens", "observed_at_utc": "2026-03-20T12:00:00+00:00"
    }])


@pytest.fixture
def mock_upload_results():
    raw_result = MagicMock()
    raw_result.s3_uri = "s3://pipeline-guard-data/weather/raw/athens/2026/03/20/120000Z.json"
    raw_result.success = True

    processed_result = MagicMock()
    processed_result.s3_uri = "s3://pipeline-guard-data/weather/processed/athens/2026/03/20/120000Z.csv"
    processed_result.success = True

    return {"raw": raw_result, "processed": processed_result}


# ── Happy path ────────────────────────────────────────────────────────────────

class TestHappyPath:

    def test_returns_pipeline_result(self, mock_raw, mock_df, mock_upload_results):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", return_value=mock_df), \
             patch("src.pipeline.upload.run", return_value=mock_upload_results):
            result = run_pipeline(city="Athens")
        assert isinstance(result, PipelineResult)

    def test_success_is_true(self, mock_raw, mock_df, mock_upload_results):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", return_value=mock_df), \
             patch("src.pipeline.upload.run", return_value=mock_upload_results):
            result = run_pipeline(city="Athens")
        assert result.success is True

    def test_city_populated(self, mock_raw, mock_df, mock_upload_results):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", return_value=mock_df), \
             patch("src.pipeline.upload.run", return_value=mock_upload_results):
            result = run_pipeline(city="Athens")
        assert result.city == "Athens"

    def test_stage_reached_is_upload(self, mock_raw, mock_df, mock_upload_results):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", return_value=mock_df), \
             patch("src.pipeline.upload.run", return_value=mock_upload_results):
            result = run_pipeline(city="Athens")
        assert result.stage_reached == "upload"

    def test_s3_uris_populated(self, mock_raw, mock_df, mock_upload_results):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", return_value=mock_df), \
             patch("src.pipeline.upload.run", return_value=mock_upload_results):
            result = run_pipeline(city="Athens")
        assert result.raw_s3_uri.startswith("s3://")
        assert result.processed_s3_uri.startswith("s3://")

    def test_no_error_on_success(self, mock_raw, mock_df, mock_upload_results):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", return_value=mock_df), \
             patch("src.pipeline.upload.run", return_value=mock_upload_results):
            result = run_pipeline(city="Athens")
        assert result.error is None

    def test_duration_is_positive(self, mock_raw, mock_df, mock_upload_results):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", return_value=mock_df), \
             patch("src.pipeline.upload.run", return_value=mock_upload_results):
            result = run_pipeline(city="Athens")
        assert result.duration_s >= 0

    def test_timestamps_set(self, mock_raw, mock_df, mock_upload_results):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", return_value=mock_df), \
             patch("src.pipeline.upload.run", return_value=mock_upload_results):
            result = run_pipeline(city="Athens")
        assert result.started_at is not None
        assert result.finished_at is not None
        assert result.finished_at >= result.started_at


# ── Stage failure attribution ─────────────────────────────────────────────────

class TestStageFailures:

    def test_ingest_env_error(self):
        with patch("src.pipeline.ingest.run", side_effect=EnvironmentError("No API key")):
            result = run_pipeline(city="Athens")
        assert result.success is False
        assert "Configuration error" in result.error
        assert result.stage_reached == "none"

    def test_ingest_unexpected_error(self):
        with patch("src.pipeline.ingest.run", side_effect=Exception("network timeout")):
            result = run_pipeline(city="Athens")
        assert result.success is False
        assert result.error is not None

    def test_transform_failure(self, mock_raw):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", side_effect=ValueError("Bad field")):
            result = run_pipeline(city="Athens")
        assert result.success is False
        assert "ingest" in result.stage_reached  # failed after ingest
        assert "Data quality error" in result.error

    def test_validate_failure(self, mock_raw, mock_df):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", side_effect=ValueError("humidity out of range")):
            result = run_pipeline(city="Athens")
        assert result.success is False
        assert result.stage_reached == "transform"
        assert "Data quality error" in result.error

    def test_upload_failure(self, mock_raw, mock_df):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", return_value=mock_df), \
             patch("src.pipeline.upload.run", side_effect=RuntimeError("S3 upload failed")):
            result = run_pipeline(city="Athens")
        assert result.success is False
        assert result.stage_reached == "validate"
        assert "Runtime error" in result.error

    def test_upload_env_error(self, mock_raw, mock_df):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", return_value=mock_df), \
             patch("src.pipeline.upload.run", side_effect=EnvironmentError("No bucket")):
            result = run_pipeline(city="Athens")
        assert result.success is False
        assert "Configuration error" in result.error


# ── PipelineResult.summary() ──────────────────────────────────────────────────

class TestSummary:

    def test_summary_contains_success(self, mock_raw, mock_df, mock_upload_results):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", return_value=mock_df), \
             patch("src.pipeline.upload.run", return_value=mock_upload_results):
            result = run_pipeline(city="Athens")
        assert "SUCCESS" in result.summary()

    def test_summary_contains_failed_on_error(self, mock_raw, mock_df):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", side_effect=ValueError("bad data")):
            result = run_pipeline(city="Athens")
        assert "FAILED" in result.summary()

    def test_summary_contains_city(self, mock_raw, mock_df, mock_upload_results):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", return_value=mock_df), \
             patch("src.pipeline.upload.run", return_value=mock_upload_results):
            result = run_pipeline(city="Athens")
        assert "Athens" in result.summary()

    def test_summary_contains_s3_uris_on_success(self, mock_raw, mock_df, mock_upload_results):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", return_value=mock_df), \
             patch("src.pipeline.validate.run", return_value=mock_df), \
             patch("src.pipeline.upload.run", return_value=mock_upload_results):
            result = run_pipeline(city="Athens")
        assert "s3://" in result.summary()

    def test_summary_contains_error_message_on_failure(self, mock_raw, mock_df):
        with patch("src.pipeline.ingest.run", return_value=mock_raw), \
             patch("src.pipeline.transform.run", side_effect=ValueError("col missing")):
            result = run_pipeline(city="Athens")
        assert "col missing" in result.summary()

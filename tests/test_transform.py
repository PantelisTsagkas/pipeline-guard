"""
tests/test_transform.py — Unit tests for transform.py

Tests cover:
- Field extraction from a realistic mock payload
- Correct DataFrame dtypes
- Edge cases: missing optional fields, bad input
"""

import pytest
import pandas as pd
from src.transform import extract_fields, to_dataframe, run


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_raw():
    """Realistic mock of an OpenWeatherMap current weather response."""
    return {
        "coord": {"lon": 23.7162, "lat": 37.9795},
        "weather": [{"id": 800, "main": "Clear", "description": "clear sky", "icon": "01d"}],
        "main": {
            "temp": 28.4,
            "feels_like": 27.9,
            "temp_min": 25.0,
            "temp_max": 30.1,
            "pressure": 1013,
            "humidity": 45,
        },
        "visibility": 10000,
        "wind": {"speed": 4.5, "deg": 180},
        "clouds": {"all": 0},
        "dt": 1720000000,
        "sys": {
            "country": "GR",
            "sunrise": 1719980000,
            "sunset": 1720030000,
        },
        "name": "Athens",
    }


@pytest.fixture
def mock_raw_no_optional(mock_raw):
    """Mock payload missing optional fields: visibility and wind deg."""
    data = mock_raw.copy()
    del data["visibility"]
    data["wind"] = {"speed": 3.1}  # no "deg"
    return data


# ── extract_fields tests ──────────────────────────────────────────────────────

class TestExtractFields:

    def test_returns_dict(self, mock_raw):
        result = extract_fields(mock_raw)
        assert isinstance(result, dict)

    def test_city_and_country(self, mock_raw):
        result = extract_fields(mock_raw)
        assert result["city"] == "Athens"
        assert result["country"] == "GR"

    def test_temperature_fields_present(self, mock_raw):
        result = extract_fields(mock_raw)
        for field in ["temp_c", "feels_like_c", "temp_min_c", "temp_max_c"]:
            assert field in result

    def test_temperature_values(self, mock_raw):
        result = extract_fields(mock_raw)
        assert result["temp_c"] == 28.4
        assert result["temp_min_c"] == 25.0
        assert result["temp_max_c"] == 30.1

    def test_humidity_in_range(self, mock_raw):
        result = extract_fields(mock_raw)
        assert 0 <= result["humidity_pct"] <= 100

    def test_weather_description(self, mock_raw):
        result = extract_fields(mock_raw)
        assert result["weather_main"] == "Clear"
        assert result["weather_desc"] == "clear sky"

    def test_optional_visibility_present(self, mock_raw):
        result = extract_fields(mock_raw)
        assert result["visibility_m"] == 10000

    def test_optional_visibility_missing(self, mock_raw_no_optional):
        result = extract_fields(mock_raw_no_optional)
        assert result["visibility_m"] is None

    def test_optional_wind_deg_missing(self, mock_raw_no_optional):
        result = extract_fields(mock_raw_no_optional)
        assert result["wind_deg"] is None

    def test_timestamps_are_strings(self, mock_raw):
        result = extract_fields(mock_raw)
        assert isinstance(result["sunrise_utc"], str)
        assert isinstance(result["sunset_utc"], str)
        assert isinstance(result["observed_at_utc"], str)

    def test_missing_required_field_raises(self, mock_raw):
        del mock_raw["main"]
        with pytest.raises(ValueError, match="Missing expected field"):
            extract_fields(mock_raw)


# ── to_dataframe tests ────────────────────────────────────────────────────────

class TestToDataframe:

    def test_returns_dataframe(self, mock_raw):
        record = extract_fields(mock_raw)
        df = to_dataframe(record)
        assert isinstance(df, pd.DataFrame)

    def test_single_row(self, mock_raw):
        record = extract_fields(mock_raw)
        df = to_dataframe(record)
        assert len(df) == 1

    def test_float_dtypes(self, mock_raw):
        record = extract_fields(mock_raw)
        df = to_dataframe(record)
        for col in ["temp_c", "feels_like_c", "wind_speed_ms", "latitude", "longitude"]:
            assert df[col].dtype == float, f"{col} should be float"

    def test_int_dtypes(self, mock_raw):
        record = extract_fields(mock_raw)
        df = to_dataframe(record)
        for col in ["humidity_pct", "pressure_hpa", "cloud_pct"]:
            assert df[col].dtype == int, f"{col} should be int"

    def test_nullable_int_for_optional(self, mock_raw_no_optional):
        record = extract_fields(mock_raw_no_optional)
        df = to_dataframe(record)
        assert df["wind_deg"].dtype == pd.Int64Dtype()
        assert pd.isna(df["wind_deg"].iloc[0])

    def test_expected_columns_present(self, mock_raw):
        record = extract_fields(mock_raw)
        df = to_dataframe(record)
        expected_cols = [
            "city", "country", "latitude", "longitude",
            "temp_c", "feels_like_c", "temp_min_c", "temp_max_c",
            "humidity_pct", "pressure_hpa", "wind_speed_ms",
            "weather_main", "weather_desc", "cloud_pct",
            "sunrise_utc", "sunset_utc", "observed_at_utc", "ingested_at_utc",
        ]
        for col in expected_cols:
            assert col in df.columns, f"Missing column: {col}"


# ── run() integration test ────────────────────────────────────────────────────

class TestRun:

    def test_run_returns_dataframe(self, mock_raw):
        df = run(mock_raw)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    def test_run_city_value(self, mock_raw):
        df = run(mock_raw)
        assert df["city"].iloc[0] == "Athens"

    def test_run_temp_value(self, mock_raw):
        df = run(mock_raw)
        assert df["temp_c"].iloc[0] == pytest.approx(28.4)

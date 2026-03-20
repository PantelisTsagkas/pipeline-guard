"""
tests/test_validate.py — Unit tests for validate.py

Tests cover:
- Valid data passes all checks
- Each category of invalid data triggers the correct failure
- Cross-column checks (temp_min <= temp_max)
- Optional nullable fields
- ValidationResult object behaviour
- run() raises ValueError on bad data
"""

import pytest
import pandas as pd
from src.validate import validate, run, ValidationResult


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def valid_df():
    """A perfectly valid, fully-populated weather DataFrame."""
    return pd.DataFrame([{
        "city":             "Athens",
        "country":          "GR",
        "latitude":         37.97,
        "longitude":        23.72,
        "temp_c":           28.3,
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
        "sunrise_utc":      "2024-07-03T04:53:20+00:00",
        "sunset_utc":       "2024-07-03T19:00:00+00:00",
        "observed_at_utc":  "2024-07-03T12:00:00+00:00",
        "ingested_at_utc":  "2024-07-03T12:01:00+00:00",
    }]).astype({
        "humidity_pct": int,
        "pressure_hpa": int,
        "cloud_pct":    int,
    })


def make_bad(valid_df, field, value):
    """Helper: return a copy of valid_df with one field set to a bad value."""
    df = valid_df.copy()
    df[field] = value
    return df


# ── Happy path ────────────────────────────────────────────────────────────────

class TestValidHappyPath:

    def test_valid_data_passes(self, valid_df):
        result = validate(valid_df)
        assert result.passed is True

    def test_valid_data_returns_dataframe(self, valid_df):
        result = validate(valid_df)
        assert isinstance(result.df, pd.DataFrame)

    def test_valid_data_no_errors(self, valid_df):
        result = validate(valid_df)
        assert result.errors == []

    def test_run_returns_dataframe_on_valid(self, valid_df):
        df = run(valid_df)
        assert isinstance(df, pd.DataFrame)

    def test_nullable_visibility_none(self, valid_df):
        valid_df["visibility_m"] = pd.array([pd.NA], dtype="Int64")
        result = validate(valid_df)
        assert result.passed is True

    def test_nullable_wind_deg_none(self, valid_df):
        valid_df["wind_deg"] = pd.array([pd.NA], dtype="Int64")
        result = validate(valid_df)
        assert result.passed is True


# ── Location checks ───────────────────────────────────────────────────────────

class TestLocationChecks:

    def test_empty_city_fails(self, valid_df):
        result = validate(make_bad(valid_df, "city", ""))
        assert result.passed is False

    def test_country_wrong_length_fails(self, valid_df):
        result = validate(make_bad(valid_df, "country", "GRC"))
        assert result.passed is False

    def test_latitude_too_high_fails(self, valid_df):
        result = validate(make_bad(valid_df, "latitude", 91.0))
        assert result.passed is False

    def test_latitude_too_low_fails(self, valid_df):
        result = validate(make_bad(valid_df, "latitude", -91.0))
        assert result.passed is False

    def test_longitude_out_of_range_fails(self, valid_df):
        result = validate(make_bad(valid_df, "longitude", 181.0))
        assert result.passed is False


# ── Temperature checks ────────────────────────────────────────────────────────

class TestTemperatureChecks:

    def test_temp_above_physical_max_fails(self, valid_df):
        result = validate(make_bad(valid_df, "temp_c", 61.0))
        assert result.passed is False

    def test_temp_below_physical_min_fails(self, valid_df):
        result = validate(make_bad(valid_df, "temp_c", -91.0))
        assert result.passed is False

    def test_feels_like_out_of_range_fails(self, valid_df):
        result = validate(make_bad(valid_df, "feels_like_c", 65.0))
        assert result.passed is False

    def test_temp_min_greater_than_temp_max_fails(self, valid_df):
        df = valid_df.copy()
        df["temp_min_c"] = 35.0
        df["temp_max_c"] = 30.0
        result = validate(df)
        assert result.passed is False

    def test_temp_min_equals_temp_max_passes(self, valid_df):
        df = valid_df.copy()
        df["temp_min_c"] = 28.0
        df["temp_max_c"] = 28.0
        result = validate(df)
        assert result.passed is True


# ── Atmosphere checks ─────────────────────────────────────────────────────────

class TestAtmosphereChecks:

    def test_humidity_above_100_fails(self, valid_df):
        result = validate(make_bad(valid_df, "humidity_pct", 101))
        assert result.passed is False

    def test_humidity_below_0_fails(self, valid_df):
        result = validate(make_bad(valid_df, "humidity_pct", -1))
        assert result.passed is False

    def test_pressure_too_low_fails(self, valid_df):
        result = validate(make_bad(valid_df, "pressure_hpa", 800))
        assert result.passed is False

    def test_pressure_too_high_fails(self, valid_df):
        result = validate(make_bad(valid_df, "pressure_hpa", 1200))
        assert result.passed is False

    def test_visibility_negative_fails(self, valid_df):
        valid_df["visibility_m"] = pd.array([-1], dtype="Int64")
        result = validate(valid_df)
        assert result.passed is False

    def test_cloud_pct_above_100_fails(self, valid_df):
        result = validate(make_bad(valid_df, "cloud_pct", 101))
        assert result.passed is False


# ── Wind checks ───────────────────────────────────────────────────────────────

class TestWindChecks:

    def test_wind_speed_negative_fails(self, valid_df):
        result = validate(make_bad(valid_df, "wind_speed_ms", -1.0))
        assert result.passed is False

    def test_wind_speed_above_max_fails(self, valid_df):
        result = validate(make_bad(valid_df, "wind_speed_ms", 121.0))
        assert result.passed is False

    def test_wind_deg_above_360_fails(self, valid_df):
        valid_df["wind_deg"] = pd.array([361], dtype="Int64")
        result = validate(valid_df)
        assert result.passed is False

    def test_wind_deg_below_0_fails(self, valid_df):
        valid_df["wind_deg"] = pd.array([-1], dtype="Int64")
        result = validate(valid_df)
        assert result.passed is False


# ── Weather condition checks ──────────────────────────────────────────────────

class TestWeatherConditionChecks:

    def test_invalid_weather_main_fails(self, valid_df):
        result = validate(make_bad(valid_df, "weather_main", "Sunshine"))
        assert result.passed is False

    def test_all_valid_weather_mains_pass(self, valid_df):
        valid_conditions = [
            "Thunderstorm", "Drizzle", "Rain", "Snow", "Mist",
            "Smoke", "Haze", "Dust", "Fog", "Sand", "Ash",
            "Squall", "Tornado", "Clear", "Clouds",
        ]
        for condition in valid_conditions:
            df = make_bad(valid_df, "weather_main", condition)
            result = validate(df)
            assert result.passed is True, f"Expected '{condition}' to pass but it failed"

    def test_empty_weather_desc_fails(self, valid_df):
        result = validate(make_bad(valid_df, "weather_desc", ""))
        assert result.passed is False


# ── Timestamp checks ──────────────────────────────────────────────────────────

class TestTimestampChecks:

    @pytest.mark.parametrize("field", ["sunrise_utc", "sunset_utc", "observed_at_utc", "ingested_at_utc"])
    def test_bad_timestamp_format_fails(self, valid_df, field):
        result = validate(make_bad(valid_df, field, "not-a-date"))
        assert result.passed is False

    @pytest.mark.parametrize("field", ["sunrise_utc", "sunset_utc", "observed_at_utc", "ingested_at_utc"])
    def test_valid_timestamp_passes(self, valid_df, field):
        result = validate(valid_df)
        assert result.passed is True


# ── ValidationResult behaviour ────────────────────────────────────────────────

class TestValidationResult:

    def test_repr_passed(self, valid_df):
        result = validate(valid_df)
        assert "PASSED" in repr(result)

    def test_repr_failed(self, valid_df):
        result = validate(make_bad(valid_df, "temp_c", 999.0))
        assert "FAILED" in repr(result)

    def test_errors_populated_on_failure(self, valid_df):
        result = validate(make_bad(valid_df, "temp_c", 999.0))
        assert len(result.errors) > 0


# ── run() raises on failure ───────────────────────────────────────────────────

class TestRunRaises:

    def test_run_raises_on_invalid_data(self, valid_df):
        bad_df = make_bad(valid_df, "humidity_pct", 150)
        with pytest.raises(ValueError, match="Data validation failed"):
            run(bad_df)

    def test_run_error_message_contains_count(self, valid_df):
        bad_df = make_bad(valid_df, "humidity_pct", 150)
        with pytest.raises(ValueError) as exc_info:
            run(bad_df)
        assert "error" in str(exc_info.value).lower()

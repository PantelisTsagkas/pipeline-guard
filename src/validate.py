"""
validate.py — Data quality validation for transformed weather DataFrames.

Uses Pandera to enforce schema contracts:
- Correct types on every column
- Value ranges based on physical weather limits
- No nulls on required fields
- String format checks on timestamps
"""

import logging
from datetime import timezone

import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ── Schema Definition ──────────────────────────────────────────────────────────

weather_schema = DataFrameSchema(
    columns={
        # Location
        "city": Column(
            str,
            nullable=False,
            checks=Check(lambda s: s.str.len() > 0, error="city must not be empty"),
        ),
        "country": Column(
            str,
            nullable=False,
            checks=Check(lambda s: s.str.len() == 2, error="country must be a 2-letter ISO code"),
        ),
        "latitude": Column(
            float,
            nullable=False,
            checks=Check.in_range(-90.0, 90.0),
        ),
        "longitude": Column(
            float,
            nullable=False,
            checks=Check.in_range(-180.0, 180.0),
        ),

        # Temperature — absolute physical limits on Earth
        "temp_c": Column(
            float,
            nullable=False,
            checks=Check.in_range(-90.0, 60.0),
        ),
        "feels_like_c": Column(
            float,
            nullable=False,
            checks=Check.in_range(-90.0, 60.0),
        ),
        "temp_min_c": Column(
            float,
            nullable=False,
            checks=[
                Check.in_range(-90.0, 60.0),
                Check(
                    lambda s, df=None: True,  # cross-column check handled below
                    error="temp_min_c must be <= temp_max_c",
                ),
            ],
        ),
        "temp_max_c": Column(
            float,
            nullable=False,
            checks=Check.in_range(-90.0, 60.0),
        ),

        # Atmosphere
        "humidity_pct": Column(
            int,
            nullable=False,
            checks=Check.in_range(0, 100),
        ),
        "pressure_hpa": Column(
            int,
            nullable=False,
            checks=Check.in_range(870, 1085),  # world record extremes
        ),
        "visibility_m": Column(
            pd.Int64Dtype(),
            nullable=True,  # optional field
            checks=Check(
                lambda s: s.dropna().between(0, 100_000).all(),
                error="visibility_m must be between 0 and 100,000 metres",
            ),
        ),

        # Wind
        "wind_speed_ms": Column(
            float,
            nullable=False,
            checks=Check.in_range(0.0, 120.0),  # max recorded ~113 m/s
        ),
        "wind_deg": Column(
            pd.Int64Dtype(),
            nullable=True,
            checks=Check(
                lambda s: s.dropna().between(0, 360).all(),
                error="wind_deg must be between 0 and 360",
            ),
        ),

        # Weather condition
        "weather_main": Column(
            str,
            nullable=False,
            checks=Check(
                lambda s: s.isin([
                    "Thunderstorm", "Drizzle", "Rain", "Snow", "Mist",
                    "Smoke", "Haze", "Dust", "Fog", "Sand", "Ash",
                    "Squall", "Tornado", "Clear", "Clouds",
                ]),
                error="weather_main must be a valid OpenWeatherMap condition",
            ),
        ),
        "weather_desc": Column(
            str,
            nullable=False,
            checks=Check(lambda s: s.str.len() > 0, error="weather_desc must not be empty"),
        ),
        "cloud_pct": Column(
            int,
            nullable=False,
            checks=Check.in_range(0, 100),
        ),

        # Timestamps — must be ISO 8601 strings
        "sunrise_utc": Column(
            str,
            nullable=False,
            checks=Check(
                lambda s: s.str.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"),
                error="sunrise_utc must be an ISO 8601 datetime string",
            ),
        ),
        "sunset_utc": Column(
            str,
            nullable=False,
            checks=Check(
                lambda s: s.str.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"),
                error="sunset_utc must be an ISO 8601 datetime string",
            ),
        ),
        "observed_at_utc": Column(
            str,
            nullable=False,
            checks=Check(
                lambda s: s.str.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"),
                error="observed_at_utc must be an ISO 8601 datetime string",
            ),
        ),
        "ingested_at_utc": Column(
            str,
            nullable=False,
            checks=Check(
                lambda s: s.str.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"),
                error="ingested_at_utc must be an ISO 8601 datetime string",
            ),
        ),
    },
    # Cross-column check: min temp must not exceed max temp
    checks=Check(
        lambda df: (df["temp_min_c"] <= df["temp_max_c"]).all(),
        error="temp_min_c must be less than or equal to temp_max_c",
    ),
    coerce=False,
    strict=False,  # allow extra columns without failing
    name="WeatherSchema",
)


# ── Validation Runner ─────────────────────────────────────────────────────────

class ValidationResult:
    """Holds the outcome of a validation run."""

    def __init__(self, passed: bool, df: pd.DataFrame = None, errors: list = None):
        self.passed = passed
        self.df = df
        self.errors = errors or []

    def __repr__(self):
        status = "PASSED" if self.passed else "FAILED"
        return f"<ValidationResult status={status} errors={len(self.errors)}>"


def validate(df: pd.DataFrame) -> ValidationResult:
    """
    Validate a transformed weather DataFrame against the weather schema.

    Args:
        df: Output from transform.run()

    Returns:
        ValidationResult with .passed, .df (if passed), and .errors (if failed)
    """
    try:
        validated_df = weather_schema.validate(df, lazy=True)
        logger.info("✅ Validation passed for all checks.")
        return ValidationResult(passed=True, df=validated_df)

    except pa.errors.SchemaErrors as e:
        error_list = e.failure_cases["failure_case"].tolist()
        check_list = e.failure_cases["check"].tolist()

        errors = [f"[{chk}] failed on value: {val}" for chk, val in zip(check_list, error_list)]

        for err in errors:
            logger.error(f"❌ {err}")

        return ValidationResult(passed=False, errors=errors)


def run(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main validation entry point. Raises on failure.

    Args:
        df: Transformed weather DataFrame

    Returns:
        Validated DataFrame if all checks pass

    Raises:
        ValueError: If any validation checks fail
    """
    result = validate(df)

    if not result.passed:
        raise ValueError(
            f"Data validation failed with {len(result.errors)} error(s):\n"
            + "\n".join(result.errors)
        )

    return result.df


if __name__ == "__main__":
    from src import ingest, transform

    raw = ingest.run()
    df = transform.run(raw)
    validated = run(df)
    print("Validation passed ✅")
    print(validated.T)

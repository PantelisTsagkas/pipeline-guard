"""
transform.py — Clean and normalize raw OpenWeatherMap JSON
into a flat, typed Pandas DataFrame row ready for validation and storage.
"""

import logging
from datetime import datetime, timezone
from typing import Union

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def extract_fields(raw: dict) -> dict:
    """
    Extract and flatten relevant fields from raw OpenWeatherMap response.

    Args:
        raw: Raw API response dictionary

    Returns:
        Flat dictionary of normalized fields
    """
    try:
        record = {
            # Location
            "city":             raw["name"],
            "country":          raw["sys"]["country"],
            "latitude":         raw["coord"]["lat"],
            "longitude":        raw["coord"]["lon"],

            # Temperature (Celsius)
            "temp_c":           raw["main"]["temp"],
            "feels_like_c":     raw["main"]["feels_like"],
            "temp_min_c":       raw["main"]["temp_min"],
            "temp_max_c":       raw["main"]["temp_max"],

            # Atmosphere
            "humidity_pct":     raw["main"]["humidity"],
            "pressure_hpa":     raw["main"]["pressure"],
            "visibility_m":     raw.get("visibility", None),

            # Wind
            "wind_speed_ms":    raw["wind"]["speed"],
            "wind_deg":         raw["wind"].get("deg", None),

            # Weather condition
            "weather_main":     raw["weather"][0]["main"],
            "weather_desc":     raw["weather"][0]["description"],
            "cloud_pct":        raw["clouds"]["all"],

            # Timestamps
            "sunrise_utc":      _unix_to_iso(raw["sys"]["sunrise"]),
            "sunset_utc":       _unix_to_iso(raw["sys"]["sunset"]),
            "observed_at_utc":  _unix_to_iso(raw["dt"]),
            "ingested_at_utc":  datetime.now(timezone.utc).isoformat(),
        }
    except KeyError as e:
        raise ValueError(f"Missing expected field in raw data: {e}")

    return record


def _unix_to_iso(unix_ts: int) -> str:
    """Convert a Unix timestamp to an ISO 8601 string (UTC)."""
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()


def to_dataframe(record: dict) -> pd.DataFrame:
    """
    Convert a single extracted record into a one-row DataFrame
    with correct dtypes.

    Args:
        record: Flat dictionary from extract_fields()

    Returns:
        Single-row DataFrame
    """
    df = pd.DataFrame([record])

    # Explicit casting for reliability
    float_cols = ["temp_c", "feels_like_c", "temp_min_c", "temp_max_c",
                  "latitude", "longitude", "wind_speed_ms"]
    int_cols   = ["humidity_pct", "pressure_hpa", "cloud_pct"]
    str_cols   = ["city", "country", "weather_main", "weather_desc",
                  "sunrise_utc", "sunset_utc", "observed_at_utc", "ingested_at_utc"]

    for col in float_cols:
        df[col] = df[col].astype(float)
    for col in int_cols:
        df[col] = df[col].astype(int)
    for col in str_cols:
        df[col] = df[col].astype(str)

    # Optional nullable int columns
    for col in ["wind_deg", "visibility_m"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    logger.info(f"Transformed record for {df['city'].iloc[0]} at {df['observed_at_utc'].iloc[0]}")
    return df


def run(raw: dict) -> pd.DataFrame:
    """
    Main transform entry point.

    Args:
        raw: Raw weather dictionary from ingest.run()

    Returns:
        Cleaned, typed single-row DataFrame
    """
    record = extract_fields(raw)
    df = to_dataframe(record)
    return df


if __name__ == "__main__":
    # Quick smoke test with a mock payload
    import json
    from pathlib import Path

    sample_path = Path(__file__).parent.parent / "data" / "raw"
    files = sorted(sample_path.glob("*.json"))
    if not files:
        print("No raw files found. Run ingest.py first.")
    else:
        with open(files[-1]) as f:
            raw = json.load(f)
        df = run(raw)
        print(df.T)  # Transpose for readable single-row display

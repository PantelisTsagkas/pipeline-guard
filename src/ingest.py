"""
ingest.py — Fetch current weather data from OpenWeatherMap API
and save raw JSON response to the data/raw/ directory.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"


def fetch_weather(city: str, api_key: str) -> dict:
    """
    Fetch current weather for a given city from OpenWeatherMap.

    Args:
        city: City name (e.g. "Athens")
        api_key: OpenWeatherMap API key

    Returns:
        Raw JSON response as a dictionary

    Raises:
        requests.HTTPError: If the API returns a non-200 status
    """
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric",  # Celsius
    }

    logger.info(f"Fetching weather data for city: {city}")
    response = requests.get(BASE_URL, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()
    logger.info(f"Successfully fetched data for {data['name']}, {data['sys']['country']}")
    return data


def save_raw(data: dict, city: str) -> Path:
    """
    Save raw API response to a timestamped JSON file.

    Args:
        data: Raw weather dictionary
        city: City name (used in filename)

    Returns:
        Path to the saved file
    """
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{city.lower().replace(' ', '_')}_{timestamp}.json"
    filepath = RAW_DATA_DIR / filename

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Raw data saved to {filepath}")
    return filepath


def run(city: str = None) -> dict:
    """
    Main ingest entry point. Fetches and saves weather data.

    Args:
        city: City name. Falls back to WEATHER_CITY env var, then 'London'.

    Returns:
        Raw weather data dictionary
    """
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise EnvironmentError("OPENWEATHER_API_KEY is not set in environment or .env file")

    city = city or os.getenv("WEATHER_CITY", "London")

    data = fetch_weather(city, api_key)
    save_raw(data, city)
    return data


if __name__ == "__main__":
    run()

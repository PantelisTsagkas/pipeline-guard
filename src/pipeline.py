"""
src/pipeline.py — Master orchestrator for Pipeline Guard.

Chains all four phases in order:
    1. Ingest   → fetch raw weather data from OpenWeatherMap
    2. Transform → normalize into a typed DataFrame
    3. Validate  → enforce data quality schema
    4. Upload    → store raw JSON + processed CSV in S3

Can be run directly:
    python -m src.pipeline
    python -m src.pipeline --city London
"""

import argparse
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from src import ingest, transform, validate, upload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ── Pipeline Run Result ────────────────────────────────────────────────────────

@dataclass
class PipelineResult:
    city:           str
    started_at:     datetime
    finished_at:    Optional[datetime] = None
    stage_reached:  str = "none"
    success:        bool = False
    raw_s3_uri:     Optional[str] = None
    processed_s3_uri: Optional[str] = None
    error:          Optional[str] = None
    duration_s:     float = 0.0

    def summary(self) -> str:
        status = "✅ SUCCESS" if self.success else "❌ FAILED"
        lines = [
            "",
            "━" * 55,
            f"  Pipeline Guard — Run Summary",
            "━" * 55,
            f"  Status       : {status}",
            f"  City         : {self.city}",
            f"  Stage reached: {self.stage_reached}",
            f"  Duration     : {self.duration_s:.2f}s",
        ]
        if self.success:
            lines += [
                f"  Raw S3 URI   : {self.raw_s3_uri}",
                f"  CSV S3 URI   : {self.processed_s3_uri}",
            ]
        if self.error:
            lines.append(f"  Error        : {self.error}")
        lines.append("━" * 55)
        return "\n".join(lines)


# ── Orchestrator ───────────────────────────────────────────────────────────────

def run_pipeline(city: str = None) -> PipelineResult:
    """
    Execute all four pipeline stages in sequence.

    Args:
        city: City name override. Falls back to WEATHER_CITY env var, then 'London'.

    Returns:
        PipelineResult with full run metadata.
    """
    started_at = datetime.now(timezone.utc)
    result = PipelineResult(city=city or "unknown", started_at=started_at)
    t0 = time.monotonic()

    try:
        # ── Stage 1: Ingest ───────────────────────────────────────────────────
        logger.info("▶ Stage 1/4: Ingest")
        raw = ingest.run(city=city)
        result.city = raw.get("name", city or "unknown")
        result.stage_reached = "ingest"

        # ── Stage 2: Transform ────────────────────────────────────────────────
        logger.info("▶ Stage 2/4: Transform")
        df = transform.run(raw)
        result.stage_reached = "transform"

        # ── Stage 3: Validate ─────────────────────────────────────────────────
        logger.info("▶ Stage 3/4: Validate")
        df = validate.run(df)
        result.stage_reached = "validate"

        # ── Stage 4: Upload ───────────────────────────────────────────────────
        logger.info("▶ Stage 4/4: Upload to S3")
        result.stage_reached = "upload"
        upload_results = upload.run(raw, df)

        result.raw_s3_uri       = upload_results["raw"].s3_uri
        result.processed_s3_uri = upload_results["processed"].s3_uri
        result.success          = True

    except EnvironmentError as e:
        result.error = f"Configuration error: {e}"
        logger.error(result.error)
    except ValueError as e:
        result.error = f"Data quality error at stage '{result.stage_reached}': {e}"
        logger.error(result.error)
    except RuntimeError as e:
        result.error = f"Runtime error at stage '{result.stage_reached}': {e}"
        logger.error(result.error)
    except Exception as e:
        result.error = f"Unexpected error at stage '{result.stage_reached}': {e}"
        logger.exception(result.error)
    finally:
        result.finished_at = datetime.now(timezone.utc)
        result.duration_s  = time.monotonic() - t0

    logger.info(result.summary())
    return result


# ── CLI Entry Point ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline Guard — Weather data pipeline with quality validation"
    )
    parser.add_argument(
        "--city",
        type=str,
        default=None,
        help="City name to fetch weather for (overrides WEATHER_CITY env var)",
    )
    args = parser.parse_args()

    result = run_pipeline(city=args.city)
    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()

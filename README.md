# Pipeline Guard 🛡️

> A production-style weather data pipeline with automated quality validation, built as a portfolio project demonstrating data engineering and test engineering skills.

![CI](https://github.com/<PantelisTsagkas/pipeline-guard/actions/workflows/pipeline.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.12-blue)
![Tests](https://img.shields.io/badge/tests-117%20passing-brightgreen)
![AWS](https://img.shields.io/badge/AWS-S3-orange)

---

## What it does

Pipeline - Guard fetches live weather data from OpenWeatherMap, transforms and validates it against a strict data quality schema, then stores the results in AWS S3 — automatically, on every push to `main` via GitHub Actions CI.

```
OpenWeatherMap API
       ↓
  ingest.py     → fetch raw JSON, save locally
       ↓
  transform.py  → normalize into a typed Pandas DataFrame
       ↓
  validate.py   → enforce quality schema (Pandera)
       ↓
  upload.py     → store raw + processed files in S3
       ↓
AWS S3: s3://pipeline-guard-data/weather/{raw|processed}/<city>/YYYY/MM/DD/
```

---

## Tech stack

| Layer | Tool |
|---|---|
| Language | Python 3.12 |
| Data processing | Pandas |
| Data validation | Pandera |
| Cloud storage | AWS S3 (boto3) |
| Testing | Pytest + pytest-cov |
| CI/CD | GitHub Actions |
| Config | python-dotenv |

---

## Project structure

```
pipeline-guard/
├── src/
│   ├── pipeline.py     # Master orchestrator — chains all stages
│   ├── ingest.py       # Fetch raw weather from OpenWeatherMap
│   ├── transform.py    # Normalize into typed DataFrame
│   ├── validate.py     # Pandera quality schema
│   └── upload.py       # Upload raw + processed files to S3
├── tests/
│   ├── test_pipeline.py
│   ├── test_transform.py
│   ├── test_validate.py
│   └── test_upload.py
├── .github/workflows/
│   └── pipeline.yml    # CI: test → run pipeline → upload to S3
├── data/raw/           # Local raw JSON cache (git-ignored)
├── .env.example
└── requirements.txt
```

---

## Data quality checks

The Pandera schema in `validate.py` enforces:

| Field | Rule |
|---|---|
| `temp_c` | Between -90°C and +60°C (Earth physical limits) |
| `temp_min_c` | Must be ≤ `temp_max_c` (cross-column check) |
| `humidity_pct` | Integer, 0–100 |
| `pressure_hpa` | 870–1085 hPa (world record extremes) |
| `wind_speed_ms` | 0–120 m/s |
| `wind_deg` | 0–360, nullable |
| `visibility_m` | 0–100,000 m, nullable |
| `weather_main` | Must be one of 15 official OWM conditions |
| `*_utc` timestamps | ISO 8601 format enforced via regex |
| `country` | Exactly 2-character ISO code |

---

## Test coverage

117 tests across 4 test files, with zero real API calls or AWS credentials needed (all external calls are mocked):

```
tests/test_transform.py   — 20 tests  (extraction, dtypes, edge cases)
tests/test_validate.py    — 42 tests  (every schema rule, cross-column checks)
tests/test_upload.py      — 36 tests  (S3 key structure, mocked boto3)
tests/test_pipeline.py    — 19 tests  (orchestration, stage failure attribution)
```

---

## CI/CD pipeline

GitHub Actions runs on every push to `main`:

1. **Test job** — installs deps, runs all 117 tests with coverage report
2. **Pipeline job** (only if tests pass) — runs the full pipeline using secrets, uploads to S3

The pipeline also runs on a **daily schedule** (08:00 UTC) to keep fresh data flowing into S3.

AWS credentials are stored as GitHub Secrets — never in code.

---

## Local setup

### 1. Clone and install

```bash
git clone https://github.com/<YOUR_USERNAME>/pipeline-guard.git
cd pipeline-guard
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Fill in: OPENWEATHER_API_KEY, WEATHER_CITY, AWS_* variables, S3_BUCKET_NAME
```

Get a free OpenWeatherMap API key at [openweathermap.org/api](https://openweathermap.org/api).

### 3. Run the tests

```bash
python -m pytest tests/ -v
```

### 4. Run the full pipeline

```bash
python -m src.pipeline --city Athens
```

---

## AWS setup

The pipeline requires an IAM user with two policies:

- `AmazonS3ReadOnlyAccess` — for listing/reading
- A custom inline policy `PipelineGuardS3Write` — least-privilege write access:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:PutObject", "s3:PutObjectAcl"],
    "Resource": "arn:aws:s3:::pipeline-guard-data/*"
  }]
}
```

S3 data is stored with Hive-style time partitioning, making it queryable with AWS Athena or Glue without any extra configuration.

---

## GitHub Actions secrets

Add these secrets in your repo under **Settings → Secrets and variables → Actions**:

| Secret | Description |
|---|---|
| `OPENWEATHER_API_KEY` | Your OpenWeatherMap API key |
| `WEATHER_CITY` | City name (e.g. `Athens`) |
| `AWS_ACCESS_KEY_ID` | IAM user access key |
| `AWS_SECRET_ACCESS_KEY` | IAM user secret key |
| `AWS_REGION` | e.g. `eu-west-1` |
| `S3_BUCKET_NAME` | e.g. `pipeline-guard-data` |

---

## Author

**Pantelis** — Apprentice Test Engineer & AWS AI Practitioner  
[GitHub](https://github.com/PantelisTsagkas) · [LinkedIn](https://linkedin.com/in/pantelis-t-6a7718249/)

"""
tests/conftest.py
=================
Shared pytest configuration and fixtures for Weather & Climate Spark tests.

NOTE ON TEST DATA:
All test fixtures use slices extracted directly from the real Open-Meteo historical
weather dataset (data/raw/india_weather_daily.csv). No synthetic or fake weather data
is invented or generated.
"""

import csv
from pathlib import Path
import sys

import pytest

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import PRIMARY_DATASET_PATH  # noqa: E402
from src.spark_session import get_spark_session  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped SparkSession fixture shared across all test modules.
    Keeps the JVM alive throughout the test run to avoid repeatedly starting Spark.
    """
    session = get_spark_session(app_name="WeatherClimate_TestSession")
    yield session
    try:
        session.stop()
    except Exception:
        pass


@pytest.fixture(scope="session")
def raw_dataset_slices():
    """
    Load real weather observations from data/raw/india_weather_daily.csv.
    Extracts the first 200 daily records for Bengaluru and 200 daily records for Mumbai.
    Strictly uses real historical weather data; no synthetic data is created.
    """
    if not PRIMARY_DATASET_PATH.exists():
        pytest.skip(f"Primary dataset not found at {PRIMARY_DATASET_PATH}")

    header: list[str] = []
    bengaluru_rows: list[list[str]] = []
    mumbai_rows: list[list[str]] = []

    with open(PRIMARY_DATASET_PATH, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        loc_idx = header.index("location")
        for row in reader:
            if not row:
                continue
            loc = row[loc_idx]
            if loc == "Bengaluru" and len(bengaluru_rows) < 200:
                bengaluru_rows.append(row)
            elif loc == "Mumbai" and len(mumbai_rows) < 200:
                mumbai_rows.append(row)
            if len(bengaluru_rows) >= 200 and len(mumbai_rows) >= 200:
                break

    return {
        "header": header,
        "bengaluru_rows": bengaluru_rows,
        "mumbai_rows": mumbai_rows,
    }


@pytest.fixture
def real_weather_slice_csv(tmp_path, raw_dataset_slices):
    """
    Write a combined CSV with 200 Bengaluru rows and 200 Mumbai rows of real weather data.
    """
    file_path = tmp_path / "real_weather_slice_400.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(raw_dataset_slices["header"])
        writer.writerows(raw_dataset_slices["bengaluru_rows"])
        writer.writerows(raw_dataset_slices["mumbai_rows"])
    return file_path

"""
tests/conftest.py
=================
Shared pytest configuration and fixtures for Weather & Climate Spark tests.
"""

import sys
from pathlib import Path

import pytest

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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


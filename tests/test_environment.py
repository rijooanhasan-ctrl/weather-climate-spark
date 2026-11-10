"""
tests/test_environment.py
=========================
Pytest suite that verifies the project environment is correctly set up.

These tests are intentionally lightweight – they check imports and that
Spark SQL can execute a trivial query.  No data files are required.

Run with:
    pytest -v
"""

import sys
from pathlib import Path

import pytest

# Make sure the project root is on the path so src/ and config/ are importable.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Import checks
# ---------------------------------------------------------------------------

class TestImports:
    """Confirm that every required package can be imported."""

    def test_pyspark_imports(self):
        """PySpark and SparkSession must be importable."""
        from pyspark.sql import SparkSession  # noqa: F401
        import pyspark  # noqa: F401
        assert pyspark.__version__.startswith("3.5"), (
            f"Expected PySpark 3.5.x, got {pyspark.__version__}"
        )

    def test_pandas_imports(self):
        """Pandas must be importable."""
        import pandas as pd  # noqa: F401
        assert pd.__version__, "Pandas version string is empty"

    def test_numpy_imports(self):
        """NumPy must be importable."""
        import numpy as np  # noqa: F401
        assert np.__version__, "NumPy version string is empty"

    def test_streamlit_imports(self):
        """Streamlit must be importable."""
        import streamlit  # noqa: F401
        assert streamlit.__version__, "Streamlit version string is empty"

    def test_plotly_imports(self):
        """Plotly must be importable."""
        import plotly  # noqa: F401
        assert plotly.__version__, "Plotly version string is empty"


# ---------------------------------------------------------------------------
# Project module checks
# ---------------------------------------------------------------------------

class TestProjectModules:
    """Confirm that our own project modules are importable and correct."""

    def test_settings_import(self):
        """config.settings must export all required constants."""
        from config.settings import (  # noqa: F401
            APP_NAME,
            BASE_DIR,
            PROCESSED_DATA_DIR,
            RAW_DATA_DIR,
            STANDARD_COLUMNS,
        )
        assert isinstance(APP_NAME, str) and APP_NAME, "APP_NAME must be a non-empty string"
        assert BASE_DIR.is_dir(), f"BASE_DIR does not exist: {BASE_DIR}"
        assert isinstance(STANDARD_COLUMNS, dict), "STANDARD_COLUMNS must be a dict"

        required_keys = {"date", "location", "temperature", "rainfall",
                         "humidity", "wind_speed", "pressure"}
        missing = required_keys - set(STANDARD_COLUMNS.keys())
        assert not missing, f"STANDARD_COLUMNS is missing keys: {missing}"

    def test_spark_session_module_import(self):
        """src.spark_session must export get_spark_session."""
        from src.spark_session import get_spark_session  # noqa: F401
        assert callable(get_spark_session)


# ---------------------------------------------------------------------------
# Spark SQL connectivity check
# ---------------------------------------------------------------------------

class TestSparkSQL:
    """
    Verify that a SparkSession can be created and that Spark SQL works.

    The session is created fresh for each test class to avoid cross-test
    interference, and stopped in the teardown.
    """

    @pytest.fixture(scope="class")
    def spark(self):
        """Create a SparkSession for this test class and stop it afterwards."""
        from src.spark_session import get_spark_session
        session = get_spark_session(app_name="WeatherClimate_TestSuite")
        yield session
        session.stop()

    def test_spark_session_created(self, spark):
        """SparkSession object must not be None."""
        assert spark is not None, "SparkSession is None"

    def test_spark_sql_select_one(self, spark):
        """
        Classic smoke test: `SELECT 1` must return a DataFrame with one row
        containing the integer 1.
        """
        df = spark.sql("SELECT 1 AS answer")
        rows = df.collect()
        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
        assert rows[0]["answer"] == 1, f"Expected answer=1, got {rows[0]['answer']}"

    def test_spark_sql_string_functions(self, spark):
        """Spark SQL string functions (UPPER, LENGTH) must work."""
        df = spark.sql("SELECT UPPER('hello') AS up, LENGTH('hello') AS len")
        row = df.collect()[0]
        assert row["up"] == "HELLO"
        assert row["len"] == 5

    def test_spark_sql_aggregation(self, spark):
        """
        Spark SQL aggregation (SUM, COUNT) using inline VALUES must work.

        Uses VALUES syntax exclusively – no Python workers are spawned, so
        this works correctly on Python 3.13 + Windows with PySpark 3.5.x.
        """
        df = spark.sql(
            "SELECT SUM(n) AS total, COUNT(n) AS cnt "
            "FROM (VALUES (1), (2), (3)) t(n)"
        )
        row = df.collect()[0]
        assert row["total"] == 6, f"SUM expected 6, got {row['total']}"
        assert row["cnt"] == 3, f"COUNT expected 3, got {row['cnt']}"


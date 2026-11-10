"""
tests/test_dataset.py
=====================
Data integrity, schema adherence, and quality tests for the primary dataset.

Executed using Apache Spark SQL only.
Skips cleanly if data/raw/india_weather_daily.csv has not yet been downloaded.
"""

import sys
from pathlib import Path

import pytest

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.schema import PLAUSIBLE_RANGES, STANDARD_SCHEMA  # noqa: E402
from config.settings import PRIMARY_DATASET_PATH  # noqa: E402
from src.utils import to_spark_path  # noqa: E402

DATASET_EXISTS = PRIMARY_DATASET_PATH.exists()
SKIP_REASON = (
    f"Primary dataset not found at {PRIMARY_DATASET_PATH}. "
    "Execute 'python scripts/download_weather_data.py' to acquire data."
)


@pytest.fixture(scope="module")
def dataset_view(spark):
    """
    Registers PRIMARY_DATASET_PATH as a temporary view for dataset tests.
    """
    if not PRIMARY_DATASET_PATH.exists():
        pytest.skip(SKIP_REASON)

    spark_path = to_spark_path(PRIMARY_DATASET_PATH)
    df = (
        spark.read
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .option("mode", "FAILFAST")
        .schema(STANDARD_SCHEMA)
        .csv(spark_path)
    )
    view_name = "test_primary_weather_raw"
    df.createOrReplaceTempView(view_name)
    return view_name


@pytest.mark.skipif(not DATASET_EXISTS, reason=SKIP_REASON)
def test_dataset_header_matches_standard_schema(spark):
    """Assert CSV header exactly matches STANDARD_SCHEMA column names in position."""
    spark_path = to_spark_path(PRIMARY_DATASET_PATH)
    header_df = spark.read.option("header", "true").csv(spark_path)
    expected_columns = [f.name for f in STANDARD_SCHEMA.fields]
    assert header_df.columns == expected_columns, (
        f"Header mismatch: {header_df.columns} != {expected_columns}"
    )


@pytest.mark.skipif(not DATASET_EXISTS, reason=SKIP_REASON)
def test_dataset_has_records(spark, dataset_view):
    """Assert dataset has a positive row count."""
    row = spark.sql(f"SELECT COUNT(*) AS total FROM {dataset_view}").collect()[0]
    assert row["total"] > 0, f"Expected total > 0, got {row['total']}"


@pytest.mark.skipif(not DATASET_EXISTS, reason=SKIP_REASON)
def test_dataset_contains_eight_locations(spark, dataset_view):
    """Assert dataset contains records for exactly 8 locations."""
    row = spark.sql(f"SELECT COUNT(DISTINCT location) AS loc_count FROM {dataset_view}").collect()[0]
    assert row["loc_count"] == 8, f"Expected exactly 8 distinct locations, found {row['loc_count']}"


@pytest.mark.skipif(not DATASET_EXISTS, reason=SKIP_REASON)
def test_dataset_zero_duplicate_keys(spark, dataset_view):
    """Assert there are zero duplicate (date, location) primary key pairs."""
    dups = spark.sql(
        f"""
        SELECT COUNT(*) AS duplicate_keys
        FROM (
            SELECT date, location
            FROM {dataset_view}
            GROUP BY date, location
            HAVING COUNT(*) > 1
        )
        """
    ).collect()[0]["duplicate_keys"]
    assert dups == 0, f"Found {dups} duplicate (date, location) records"


@pytest.mark.skipif(not DATASET_EXISTS, reason=SKIP_REASON)
def test_dataset_no_values_outside_plausible_ranges(spark, dataset_view):
    """Assert zero values exceed the physical plausibility ranges for numeric metrics."""
    clauses = [
        f"SUM(CASE WHEN {col} < {low} OR {col} > {high} THEN 1 ELSE 0 END) AS {col}_outliers"
        for col, (low, high) in PLAUSIBLE_RANGES.items()
    ]
    query = f"SELECT {', '.join(clauses)} FROM {dataset_view}"
    res = spark.sql(query).collect()[0]

    for col in PLAUSIBLE_RANGES:
        outlier_count = res[f"{col}_outliers"]
        assert outlier_count == 0, (
            f"Column '{col}' has {outlier_count} values outside plausible range {PLAUSIBLE_RANGES[col]}"
        )


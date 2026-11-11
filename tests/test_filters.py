"""
tests/test_filters.py
=====================
Tests for the analytical query filtering system (Filters and build_where).

Verifies:
- Injection safety: quotes, backslashes, and classic SQL injection payloads
- Empty location list returning zero rows (1 = 0)
- Date range, year, and month clause building and filtering
- Input validation: friendly ValueErrors on invalid date or number formats
"""

from __future__ import annotations

from datetime import date
import pytest

from config.settings import PRIMARY_DATASET_PATH
from src.pipeline import prepare
from src.queries.base import Filters, build_where
from src.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("TestFiltersSession")


@pytest.fixture(scope="session")
def filter_view(spark):
    view_name = "weather_clean_test_filters"
    prepare(spark, path=PRIMARY_DATASET_PATH, view_name=view_name)
    return view_name


def test_filters_invalid_dates():
    """Invalid date strings must raise a clear ValueError, not a silent failure or SQL error."""
    with pytest.raises(ValueError, match="Invalid start_date"):
        Filters(start_date="not-a-date")

    with pytest.raises(ValueError, match="Invalid start_date"):
        Filters(start_date="2020-02-31")

    with pytest.raises(ValueError, match="Invalid end_date"):
        Filters(end_date="2020-13-01")


def test_filters_invalid_numeric_ranges():
    """Invalid month or year values must raise a clear ValueError."""
    with pytest.raises(ValueError, match="Invalid month"):
        Filters(months=[0])

    with pytest.raises(ValueError, match="Invalid month"):
        Filters(months=[13])

    with pytest.raises(ValueError, match="Invalid year"):
        Filters(years=["not_a_year"])


def test_filters_empty_location_list(spark, filter_view):
    """An explicitly empty location list ([]) must produce 1 = 0 and return zero rows."""
    f = Filters(locations=[])
    where_sql = build_where(f)
    assert where_sql == "1 = 0"

    df = spark.sql(f"SELECT * FROM {filter_view} WHERE {where_sql}")
    assert df.count() == 0


def test_filters_sql_injection_safety(spark, filter_view):
    """
    SQL injection attempts and unusual string literals must be safely escaped,
    execute without syntax errors, and return zero rows when no matching station exists.
    """
    malicious_inputs = [
        "O'Brien",
        "Delhi\\East",
        "x' OR '1'='1",
        "'; DROP TABLE weather_clean; --",
        'Station"With"Quotes',
    ]

    for attack in malicious_inputs:
        f = Filters(locations=[attack])
        where_sql = build_where(f)
        # Must execute cleanly on Spark SQL without syntax error
        df = spark.sql(f"SELECT * FROM {filter_view} WHERE {where_sql}")
        assert df.count() == 0


def test_filters_date_and_location_filtering(spark, filter_view):
    """Filters must accurately restrict rows by location, date range, years, and months."""
    # 1. Location filter (Bengaluru has 365 * 12 + 366 * 4 = 5,844 rows across 16 years)
    f_loc = Filters(locations=["Bengaluru"])
    df_loc = spark.sql(f"SELECT * FROM {filter_view} WHERE {build_where(f_loc)}")
    assert df_loc.count() == 5844

    # 2. Year filter (2015 has 365 * 8 = 2,920 rows across 8 cities)
    f_year = Filters(years=[2015])
    df_year = spark.sql(f"SELECT * FROM {filter_view} WHERE {build_where(f_year)}")
    assert df_year.count() == 2920

    # 3. Date range filter
    f_range = Filters(start_date="2020-01-01", end_date="2020-01-31", locations=["Mumbai"])
    df_range = spark.sql(f"SELECT * FROM {filter_view} WHERE {build_where(f_range)}")
    assert df_range.count() == 31

    # 4. Multi-location and month filter
    f_multi = Filters(locations=["Mumbai", "Delhi"], months=[1], years=[2021])
    df_multi = spark.sql(f"SELECT * FROM {filter_view} WHERE {build_where(f_multi)}")
    # 31 days in Jan * 2 locations = 62
    assert df_multi.count() == 62

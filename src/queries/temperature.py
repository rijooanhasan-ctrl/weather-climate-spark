"""
src/queries/temperature.py
==========================
Pure Spark SQL queries for temperature trends, climatology, annual anomalies,
and seasonal patterns.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from config.settings import (
    CLEAN_VIEW_NAME,
    COMPLETE_YEAR_MIN_COVERAGE,
    TREND_MIN_YEARS,
)
from src.queries.base import Filters, QueryResult, build_where, run_query


def temp_yearly(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Annual temperature aggregates, coverage, baseline anomalies, and year-over-year changes.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT location, year, temperature, temp_max, temp_min
        FROM {view}
        WHERE {where_clause}
    ),
    yearly_agg AS (
        SELECT
            location,
            year,
            ROUND(AVG(temperature), 2) AS avg_temp,
            ROUND(AVG(temp_max), 2) AS avg_temp_max,
            ROUND(AVG(temp_min), 2) AS avg_temp_min,
            ROUND(MAX(temp_max), 2) AS record_max,
            ROUND(MIN(temp_min), 2) AS record_min,
            COUNT(temperature) AS days_with_data,
            DATEDIFF(make_date(year + 1, 1, 1), make_date(year, 1, 1)) AS days_in_year,
            CASE
                WHEN (CAST(COUNT(temperature) AS DOUBLE) / DATEDIFF(make_date(year + 1, 1, 1), make_date(year, 1, 1))) >= {COMPLETE_YEAR_MIN_COVERAGE}
                THEN true
                ELSE false
            END AS is_complete_year
        FROM base
        GROUP BY location, year
    ),
    with_baseline AS (
        SELECT
            location,
            year,
            avg_temp,
            avg_temp_max,
            avg_temp_min,
            record_max,
            record_min,
            days_with_data,
            is_complete_year,
            ROUND(avg_temp - AVG(avg_temp) OVER (PARTITION BY location), 2) AS anomaly_vs_location_mean,
            ROUND(avg_temp - LAG(avg_temp, 1) OVER (PARTITION BY location ORDER BY year), 2) AS yoy_change
        FROM yearly_agg
    )
    SELECT
        location,
        year,
        avg_temp,
        avg_temp_max,
        avg_temp_min,
        record_max,
        record_min,
        days_with_data,
        is_complete_year,
        anomaly_vs_location_mean,
        yoy_change
    FROM with_baseline
    ORDER BY location, year
    """
    return run_query(
        spark=spark,
        key="temp_yearly",
        title="Annual Temperature Summary & YoY Anomalies",
        description="Yearly temperature metrics, completeness flags, baseline deviations, and year-over-year changes.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def temp_monthly_climatology(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Long-term monthly temperature climatology per location.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    )
    SELECT
        location,
        month,
        ROUND(AVG(temperature), 2) AS avg_temp,
        ROUND(AVG(temp_max), 2) AS avg_temp_max,
        ROUND(AVG(temp_min), 2) AS avg_temp_min
    FROM base
    GROUP BY location, month
    ORDER BY location, month
    """
    return run_query(
        spark=spark,
        key="temp_monthly_climatology",
        title="Monthly Temperature Climatology",
        description="Multi-year average temperature, mean maximum, and mean minimum for each calendar month.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def temp_monthly_series(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Continuous monthly temperature time series with 12-month rolling average.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    ),
    monthly_agg AS (
        SELECT
            location,
            year,
            month,
            ROUND(AVG(temperature), 2) AS avg_temp,
            ROW_NUMBER() OVER (PARTITION BY location ORDER BY year, month) AS month_idx,
            AVG(AVG(temperature)) OVER (
                PARTITION BY location
                ORDER BY year, month
                ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ) AS rolling_12m_raw
        FROM base
        GROUP BY location, year, month
    )
    SELECT
        location,
        year,
        month,
        avg_temp,
        CASE
            WHEN month_idx >= 12 THEN ROUND(rolling_12m_raw, 2)
            ELSE NULL
        END AS rolling_12m_avg
    FROM monthly_agg
    ORDER BY location, year, month
    """
    return run_query(
        spark=spark,
        key="temp_monthly_series",
        title="Monthly Temperature Time Series & 12M Rolling Average",
        description="Continuous monthly temperature series with a trailing 12-month smoothing filter.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def temp_location_ranking(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Thermal ranking of locations with all-time historical temperature extremes and dates.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT location, date, temperature, temp_max, temp_min
        FROM {view}
        WHERE {where_clause}
    ),
    loc_summary AS (
        SELECT
            location,
            ROUND(AVG(temperature), 2) AS avg_temp,
            MAX(temp_max) AS max_val,
            MIN(temp_min) AS min_val
        FROM base
        GROUP BY location
    ),
    extremes_dates AS (
        SELECT
            b.location,
            FIRST(CASE WHEN b.temp_max = s.max_val THEN CAST(b.date AS STRING) END, true) AS record_max_date,
            FIRST(CASE WHEN b.temp_min = s.min_val THEN CAST(b.date AS STRING) END, true) AS record_min_date
        FROM base b
        JOIN loc_summary s ON b.location = s.location
        GROUP BY b.location
    )
    SELECT
        s.location,
        s.avg_temp,
        ROUND(s.max_val, 2) AS record_max,
        d.record_max_date,
        ROUND(s.min_val, 2) AS record_min,
        d.record_min_date,
        RANK() OVER (ORDER BY s.avg_temp DESC, s.location ASC) AS hottest_rank,
        RANK() OVER (ORDER BY s.avg_temp ASC, s.location ASC) AS coldest_rank
    FROM loc_summary s
    JOIN extremes_dates d ON s.location = d.location
    ORDER BY hottest_rank, s.location ASC
    """
    return run_query(
        spark=spark,
        key="temp_location_ranking",
        title="Location Temperature Rankings & Record Extremes",
        description="Thermal comparison of stations ordered by overall mean temperature with record dates.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def temp_trend(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Multi-decadal temperature trend analysis computed via linear regression on complete years only.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT location, year, temperature
        FROM {view}
        WHERE {where_clause}
    ),
    all_locations AS (
        SELECT DISTINCT location FROM base
    ),
    yearly_agg AS (
        SELECT
            location,
            year,
            AVG(temperature) AS avg_temp,
            CASE
                WHEN (CAST(COUNT(temperature) AS DOUBLE) / DATEDIFF(make_date(year + 1, 1, 1), make_date(year, 1, 1))) >= {COMPLETE_YEAR_MIN_COVERAGE}
                THEN true
                ELSE false
            END AS is_complete_year
        FROM base
        GROUP BY location, year
    ),
    complete_years AS (
        SELECT location, year, avg_temp
        FROM yearly_agg
        WHERE is_complete_year = true
    ),
    loc_regr AS (
        SELECT
            location,
            regr_slope(avg_temp, CAST(year AS DOUBLE)) AS raw_slope,
            regr_r2(avg_temp, CAST(year AS DOUBLE)) AS raw_r2,
            COUNT(*) AS n_years
        FROM complete_years
        GROUP BY location
    ),
    loc_endpoints AS (
        SELECT
            location,
            MAX(CASE WHEN rn_asc = 1 THEN avg_temp END) AS first_year_avg,
            MAX(CASE WHEN rn_desc = 1 THEN avg_temp END) AS last_year_avg
        FROM (
            SELECT
                location,
                avg_temp,
                ROW_NUMBER() OVER (PARTITION BY location ORDER BY year ASC) AS rn_asc,
                ROW_NUMBER() OVER (PARTITION BY location ORDER BY year DESC) AS rn_desc
            FROM complete_years
        )
        GROUP BY location
    ),
    loc_distinct AS (
        SELECT
            r.location,
            ROUND(r.raw_slope, 4) AS slope_c_per_year,
            ROUND(r.raw_slope * 10, 3) AS slope_per_decade,
            ROUND(COALESCE(r.raw_r2, 0.0), 4) AS r_squared,
            r.n_years,
            ROUND(e.first_year_avg, 2) AS first_year_avg,
            ROUND(e.last_year_avg, 2) AS last_year_avg
        FROM loc_regr r
        JOIN loc_endpoints e ON r.location = e.location
    ),
    loc_distinct_all AS (
        SELECT
            a.location,
            COALESCE(d.slope_c_per_year, 0.0) AS slope_c_per_year,
            COALESCE(d.slope_per_decade, 0.0) AS slope_per_decade,
            COALESCE(d.r_squared, 0.0) AS r_squared,
            COALESCE(d.n_years, 0) AS n_years,
            d.first_year_avg,
            d.last_year_avg
        FROM all_locations a
        LEFT JOIN loc_distinct d ON a.location = d.location
    ),
    all_loc_yearly AS (
        SELECT
            year,
            AVG(avg_temp) AS avg_temp
        FROM complete_years
        GROUP BY year
    ),
    all_loc_regr AS (
        SELECT
            'All locations' AS location,
            regr_slope(avg_temp, CAST(year AS DOUBLE)) AS raw_slope,
            regr_r2(avg_temp, CAST(year AS DOUBLE)) AS raw_r2,
            COUNT(*) AS n_years
        FROM all_loc_yearly
    ),
    all_loc_endpoints AS (
        SELECT
            'All locations' AS location,
            MAX(CASE WHEN rn_asc = 1 THEN avg_temp END) AS first_year_avg,
            MAX(CASE WHEN rn_desc = 1 THEN avg_temp END) AS last_year_avg
        FROM (
            SELECT
                avg_temp,
                ROW_NUMBER() OVER (ORDER BY year ASC) AS rn_asc,
                ROW_NUMBER() OVER (ORDER BY year DESC) AS rn_desc
            FROM all_loc_yearly
        )
    ),
    all_loc_distinct AS (
        SELECT
            r.location,
            ROUND(COALESCE(r.raw_slope, 0.0), 4) AS slope_c_per_year,
            ROUND(COALESCE(r.raw_slope, 0.0) * 10, 3) AS slope_per_decade,
            ROUND(COALESCE(r.raw_r2, 0.0), 4) AS r_squared,
            r.n_years,
            ROUND(e.first_year_avg, 2) AS first_year_avg,
            ROUND(e.last_year_avg, 2) AS last_year_avg
        FROM all_loc_regr r
        JOIN all_loc_endpoints e ON r.location = e.location
    ),
    combined AS (
        SELECT * FROM loc_distinct_all
        UNION ALL
        SELECT * FROM all_loc_distinct
    )
    SELECT
        location,
        slope_c_per_year,
        slope_per_decade,
        r_squared,
        n_years,
        first_year_avg,
        last_year_avg,
        CASE
            WHEN n_years < {TREND_MIN_YEARS} THEN 'insufficient data'
            WHEN r_squared < 0.10 THEN 'no clear trend'
            WHEN slope_c_per_year > 0 THEN 'warming'
            ELSE 'cooling'
        END AS trend_label
    FROM combined
    ORDER BY CASE WHEN location = 'All locations' THEN 1 ELSE 0 END, location ASC
    """
    return run_query(
        spark=spark,
        key="temp_trend",
        title="Long-Term Temperature Trends (Linear Regression)",
        description="Ordinary least squares temperature trends per decade and overall regional trend for complete years.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def temp_seasonal(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    by_year: bool = False,
    **params: Any,
) -> QueryResult:
    """
    Seasonal temperature climatology across Indian meteorological seasons.
    """
    where_clause = build_where(filters)
    if by_year:
        sql = f"""
        WITH base AS (
            SELECT *
            FROM {view}
            WHERE {where_clause}
        )
        SELECT
            location,
            year,
            season,
            ROUND(AVG(temperature), 2) AS avg_temp,
            ROUND(AVG(temp_max), 2) AS avg_temp_max,
            ROUND(AVG(temp_min), 2) AS avg_temp_min
        FROM base
        GROUP BY location, year, season
        ORDER BY location, year, season
        """
    else:
        sql = f"""
        WITH base AS (
            SELECT *
            FROM {view}
            WHERE {where_clause}
        )
        SELECT
            location,
            season,
            ROUND(AVG(temperature), 2) AS avg_temp,
            ROUND(AVG(temp_max), 2) AS avg_temp_max,
            ROUND(AVG(temp_min), 2) AS avg_temp_min
        FROM base
        GROUP BY location, season
        ORDER BY location, season
        """
    return run_query(
        spark=spark,
        key="temp_seasonal",
        title="Seasonal Temperature Summary",
        description="Temperature means and extremes broken down by meteorological season.",
        sql=sql,
        parameters={"filters": str(filters), "by_year": by_year},
    )

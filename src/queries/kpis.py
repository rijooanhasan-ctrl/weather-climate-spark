"""
src/queries/kpis.py
===================
Executive climate summary KPIs and headline indicator metrics computed in pure Spark SQL.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from config.settings import CLEAN_VIEW_NAME, FILL_COLUMNS
from src.queries.base import Filters, QueryResult, build_where, run_query


def kpi_summary(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    High-level climate KPI dashboard summary card metrics.
    """
    where_clause = build_where(filters)
    fill_flags_sum = " + ".join([f"COALESCE(SUM(CASE WHEN `{col}_was_filled` THEN 1 ELSE 0 END), 0)" for col in FILL_COLUMNS])

    sql = f"""
    WITH base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    ),
    headline_aggs AS (
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT location) AS locations,
            CAST(MIN(date) AS STRING) AS first_date,
            CAST(MAX(date) AS STRING) AS last_date,
            MIN(date) AS raw_first_date,
            MAX(date) AS raw_last_date,
            ROUND(AVG(temperature), 2) AS avg_temp,
            MAX(temp_max) AS max_temp_raw,
            MIN(temp_min) AS min_temp_raw,
            ROUND(COALESCE(SUM(rainfall), 0.0), 2) AS total_rainfall_mm,
            ROUND(AVG(humidity), 2) AS avg_humidity,
            ROUND(AVG(wind_speed), 2) AS avg_wind_speed,
            ROUND(AVG(pressure), 2) AS avg_pressure,
            {fill_flags_sum} AS interpolated_value_count
        FROM base
    ),
    hottest_day AS (
        SELECT
            CAST(date AS STRING) AS max_temp_date,
            location AS max_temp_location
        FROM base
        WHERE temp_max = (SELECT max_temp_raw FROM headline_aggs)
        ORDER BY date ASC, location ASC
        LIMIT 1
    ),
    coldest_day AS (
        SELECT
            CAST(date AS STRING) AS min_temp_date,
            location AS min_temp_location
        FROM base
        WHERE temp_min = (SELECT min_temp_raw FROM headline_aggs)
        ORDER BY date ASC, location ASC
        LIMIT 1
    ),
    yearly_totals AS (
        SELECT
            year,
            AVG(temperature) AS y_temp,
            SUM(rainfall) AS y_rain
        FROM base
        GROUP BY year
    ),
    hottest_y AS (
        SELECT year AS hottest_year
        FROM yearly_totals
        ORDER BY y_temp DESC, year ASC
        LIMIT 1
    ),
    wettest_y AS (
        SELECT year AS wettest_year
        FROM yearly_totals
        ORDER BY y_rain DESC, year ASC
        LIMIT 1
    )
    SELECT
        h.total_records,
        h.locations,
        h.first_date,
        h.last_date,
        h.avg_temp,
        ROUND(h.max_temp_raw, 2) AS max_temp,
        t_hot.max_temp_date,
        t_hot.max_temp_location,
        ROUND(h.min_temp_raw, 2) AS min_temp,
        t_cold.min_temp_date,
        t_cold.min_temp_location,
        h.total_rainfall_mm,
        ROUND(
            h.total_rainfall_mm / (
                NULLIF(h.locations, 0) * (
                    GREATEST(DATEDIFF(h.raw_last_date, h.raw_first_date) + 1, 1) / 365.25
                )
            ),
            2
        ) AS avg_annual_rainfall_per_location,
        h.avg_humidity,
        h.avg_wind_speed,
        h.avg_pressure,
        wy.wettest_year,
        hy.hottest_year,
        h.interpolated_value_count
    FROM headline_aggs h
    CROSS JOIN hottest_day t_hot
    CROSS JOIN coldest_day t_cold
    CROSS JOIN hottest_y hy
    CROSS JOIN wettest_y wy
    """
    return run_query(
        spark=spark,
        key="kpi_summary",
        title="Executive Climate KPI Headline Summary",
        description="Comprehensive summary metrics covering span, station counts, mean parameters, records, and data quality flags.",
        sql=sql,
        parameters={"filters": str(filters)},
    )

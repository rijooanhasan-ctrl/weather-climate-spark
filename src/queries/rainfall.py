"""
src/queries/rainfall.py
=======================
Pure Spark SQL queries for rainfall patterns, precipitation climatology, seasonal
distributions, unusual precipitation anomalies (Z-scores), and IMD heavy rainfall events.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from config.settings import (
    CLEAN_VIEW_NAME,
    COMPLETE_YEAR_MIN_COVERAGE,
    EXTREMELY_HEAVY_RAIN_MM,
    HEAVY_RAIN_MM,
    MIN_YEARS_FOR_ZSCORE,
    RAINFALL_ZSCORE_THRESHOLD,
    RAINY_DAY_MM,
    VERY_HEAVY_RAIN_MM,
)
from src.queries.base import Filters, QueryResult, build_where, run_query


def rain_monthly(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Monthly total precipitation, rainy day counts, and daily rainfall extremes.
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
        year,
        month,
        ROUND(COALESCE(SUM(rainfall), 0.0), 2) AS total_rain,
        COUNT(CASE WHEN rainfall >= {RAINY_DAY_MM} THEN 1 END) AS rainy_days,
        ROUND(COALESCE(MAX(rainfall), 0.0), 2) AS max_daily,
        COUNT(rainfall) AS days_with_data
    FROM base
    GROUP BY location, year, month
    ORDER BY location, year, month
    """
    return run_query(
        spark=spark,
        key="rain_monthly",
        title="Monthly Rainfall Aggregates",
        description="Monthly accumulated rainfall, rainy days (>= 2.5 mm), and daily peak rainfall.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def rain_yearly(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Annual rainfall accumulation, rainy days count, completeness, and YoY changes.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    ),
    yearly_agg AS (
        SELECT
            location,
            year,
            ROUND(COALESCE(SUM(rainfall), 0.0), 2) AS total_rain,
            COUNT(CASE WHEN rainfall >= {RAINY_DAY_MM} THEN 1 END) AS rainy_days,
            ROUND(COALESCE(MAX(rainfall), 0.0), 2) AS max_daily,
            COUNT(rainfall) AS days_with_data,
            DATEDIFF(make_date(year + 1, 1, 1), make_date(year, 1, 1)) AS days_in_year,
            CASE
                WHEN (CAST(COUNT(rainfall) AS DOUBLE) / DATEDIFF(make_date(year + 1, 1, 1), make_date(year, 1, 1))) >= {COMPLETE_YEAR_MIN_COVERAGE}
                THEN true
                ELSE false
            END AS is_complete_year
        FROM base
        GROUP BY location, year
    )
    SELECT
        location,
        year,
        total_rain,
        rainy_days,
        max_daily,
        days_with_data,
        is_complete_year,
        ROUND(total_rain - LAG(total_rain, 1) OVER (PARTITION BY location ORDER BY year), 2) AS yoy_change
    FROM yearly_agg
    ORDER BY location, year
    """
    return run_query(
        spark=spark,
        key="rain_yearly",
        title="Annual Rainfall Totals & YoY Shifts",
        description="Yearly rainfall volume, rainy days count, data completeness, and annual change.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def rain_monthly_climatology(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Long-term average rainfall volume for each calendar month across years.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    ),
    yearly_monthly AS (
        SELECT
            location,
            year,
            month,
            SUM(rainfall) AS monthly_sum
        FROM base
        GROUP BY location, year, month
    )
    SELECT
        location,
        month,
        ROUND(AVG(monthly_sum), 2) AS avg_monthly_total
    FROM yearly_monthly
    GROUP BY location, month
    ORDER BY location, month
    """
    return run_query(
        spark=spark,
        key="rain_monthly_climatology",
        title="Monthly Rainfall Climatology",
        description="Historical mean rainfall accumulation for each calendar month.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def rain_location_ranking(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Location rainfall ranking by average annual volume, evaluated strictly on complete years.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    ),
    complete_years AS (
        SELECT
            location,
            year,
            SUM(rainfall) AS annual_rain
        FROM base
        GROUP BY location, year
        HAVING (CAST(COUNT(rainfall) AS DOUBLE) / DATEDIFF(make_date(year + 1, 1, 1), make_date(year, 1, 1))) >= {COMPLETE_YEAR_MIN_COVERAGE}
    ),
    loc_annual AS (
        SELECT
            location,
            ROUND(AVG(annual_rain), 2) AS avg_annual_rain,
            COUNT(*) AS complete_years_used
        FROM complete_years
        GROUP BY location
    ),
    monthly_climo AS (
        SELECT
            location,
            month,
            AVG(monthly_sum) AS avg_m
        FROM (
            SELECT location, year, month, SUM(rainfall) AS monthly_sum
            FROM base
            GROUP BY location, year, month
        )
        GROUP BY location, month
    ),
    wettest_m AS (
        SELECT
            location,
            FIRST(month) OVER (PARTITION BY location ORDER BY avg_m DESC, month ASC) AS wettest_month
        FROM monthly_climo
    ),
    wettest_m_distinct AS (
        SELECT location, MAX(wettest_month) AS wettest_month
        FROM wettest_m
        GROUP BY location
    )
    SELECT
        a.location,
        a.avg_annual_rain,
        w.wettest_month,
        RANK() OVER (ORDER BY a.avg_annual_rain DESC, a.location ASC) AS rank,
        a.complete_years_used
    FROM loc_annual a
    JOIN wettest_m_distinct w ON a.location = w.location
    ORDER BY rank, a.location ASC
    """
    return run_query(
        spark=spark,
        key="rain_location_ranking",
        title="Location Rainfall Volume Ranking",
        description="Stations ranked by average annual rainfall computed exclusively over complete calendar years.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def rain_seasonal(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Seasonal rainfall distribution and percentage contribution to annual total over complete years.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    ),
    complete_years_list AS (
        SELECT
            location,
            year
        FROM base
        GROUP BY location, year
        HAVING (CAST(COUNT(rainfall) AS DOUBLE) / DATEDIFF(make_date(year + 1, 1, 1), make_date(year, 1, 1))) >= {COMPLETE_YEAR_MIN_COVERAGE}
    ),
    complete_base AS (
        SELECT b.*
        FROM base b
        JOIN complete_years_list c ON b.location = c.location AND b.year = c.year
    ),
    seasonal_totals AS (
        SELECT
            location,
            year,
            season,
            SUM(rainfall) AS seasonal_rain
        FROM complete_base
        GROUP BY location, year, season
    ),
    loc_seasonal_avg AS (
        SELECT
            location,
            season,
            ROUND(AVG(seasonal_rain), 2) AS avg_seasonal_total
        FROM seasonal_totals
        GROUP BY location, season
    ),
    with_shares AS (
        SELECT
            location,
            season,
            avg_seasonal_total,
            ROUND(
                (avg_seasonal_total / NULLIF(SUM(avg_seasonal_total) OVER (PARTITION BY location), 0.0)) * 100.0,
                2
            ) AS share_of_annual_pct
        FROM loc_seasonal_avg
    )
    SELECT
        location,
        season,
        avg_seasonal_total,
        COALESCE(share_of_annual_pct, 0.0) AS share_of_annual_pct
    FROM with_shares
    ORDER BY location, season
    """
    return run_query(
        spark=spark,
        key="rain_seasonal",
        title="Seasonal Rainfall Totals & Annual Share",
        description="Average precipitation per meteorological season and its percentage share of annual volume.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def rain_unusual_months(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Detection of unusually wet months via historical multi-year Z-scores.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    ),
    monthly_sums AS (
        SELECT
            location,
            year,
            month,
            ROUND(SUM(rainfall), 2) AS total_rain
        FROM base
        GROUP BY location, year, month
    ),
    month_stats AS (
        SELECT
            location,
            month,
            AVG(total_rain) AS month_mean,
            STDDEV_SAMP(total_rain) AS month_std,
            COUNT(*) AS n_years
        FROM monthly_sums
        GROUP BY location, month
    )
    SELECT
        m.location,
        m.year,
        m.month,
        m.total_rain,
        ROUND(s.month_mean, 2) AS month_mean,
        ROUND(s.month_std, 2) AS month_std,
        CASE
            WHEN s.month_std > 0 AND s.n_years >= {MIN_YEARS_FOR_ZSCORE}
            THEN ROUND((m.total_rain - s.month_mean) / s.month_std, 2)
            ELSE NULL
        END AS z_score,
        CASE
            WHEN s.month_std > 0 AND s.n_years >= {MIN_YEARS_FOR_ZSCORE} AND ((m.total_rain - s.month_mean) / s.month_std) >= {RAINFALL_ZSCORE_THRESHOLD}
            THEN true
            ELSE false
        END AS is_unusually_high
    FROM monthly_sums m
    JOIN month_stats s ON m.location = s.location AND m.month = s.month
    ORDER BY m.location, m.year, m.month
    """
    return run_query(
        spark=spark,
        key="rain_unusual_months",
        title="Unusual Rainfall Months (Z-Score Analysis)",
        description=f"Identifies months with precipitation >= {RAINFALL_ZSCORE_THRESHOLD} standard deviations above the historical station monthly mean.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def rain_heavy_days(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Annual counts of severe rainfall days categorized by official IMD intensity thresholds.
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
        year,
        COUNT(CASE WHEN rainfall >= {HEAVY_RAIN_MM} AND rainfall < {VERY_HEAVY_RAIN_MM} THEN 1 END) AS heavy_rain_days,
        COUNT(CASE WHEN rainfall >= {VERY_HEAVY_RAIN_MM} AND rainfall < {EXTREMELY_HEAVY_RAIN_MM} THEN 1 END) AS very_heavy_rain_days,
        COUNT(CASE WHEN rainfall >= {EXTREMELY_HEAVY_RAIN_MM} THEN 1 END) AS extremely_heavy_rain_days,
        COUNT(CASE WHEN rainfall >= {HEAVY_RAIN_MM} THEN 1 END) AS total_heavy_days
    FROM base
    GROUP BY location, year
    ORDER BY location, year
    """
    return run_query(
        spark=spark,
        key="rain_heavy_days",
        title="IMD Extreme Rainfall Days Frequency",
        description=f"Days classified under IMD brackets: Heavy ({HEAVY_RAIN_MM}-{VERY_HEAVY_RAIN_MM} mm), Very Heavy ({VERY_HEAVY_RAIN_MM}-{EXTREMELY_HEAVY_RAIN_MM} mm), and Extremely Heavy (>={EXTREMELY_HEAVY_RAIN_MM} mm).",
        sql=sql,
        parameters={"filters": str(filters)},
    )

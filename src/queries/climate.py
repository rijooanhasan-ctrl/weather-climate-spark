"""
src/queries/climate.py
======================
Pure Spark SQL queries for multi-variate climate profiles, yearly comparisons,
extreme thresholds (percentiles), heatwave spell detection (gaps-and-islands),
and variable correlations.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from config.settings import (
    CLEAN_VIEW_NAME,
    COLD_PERCENTILE,
    CORRELATION_VARIABLES,
    HEATWAVE_MIN_DAYS,
    HEAVY_RAIN_MM,
    HOT_PERCENTILE,
    PERCENTILE_ACCURACY,
    RAINY_DAY_MM,
    TOP_N_DEFAULT,
    WET_DAY_EXTREME_PERCENTILE,
)
from src.queries.base import Filters, QueryResult, build_where, run_query


def climate_year_profile(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Multi-variate annual climate profiles averaged across selected stations.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    )
    SELECT
        year,
        ROUND(AVG(temperature), 2) AS avg_temp,
        ROUND(SUM(rainfall) / NULLIF(COUNT(DISTINCT location), 0), 2) AS avg_rain_per_location,
        ROUND(AVG(humidity), 2) AS avg_humidity,
        ROUND(AVG(wind_speed), 2) AS avg_wind_speed,
        ROUND(AVG(pressure), 2) AS avg_pressure,
        COUNT(DISTINCT date) AS days
    FROM base
    GROUP BY year
    ORDER BY year
    """
    return run_query(
        spark=spark,
        key="climate_year_profile",
        title="Annual Multi-Variate Climate Profile",
        description="Composite yearly climate metrics across all monitored atmospheric variables.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def climate_year_comparison(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    year_a: int = 2015,
    year_b: int = 2020,
    **params: Any,
) -> QueryResult:
    """
    Head-to-head comparison of all meteorological parameters between two calendar years.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    ),
    year_stats AS (
        SELECT
            year,
            COUNT(DISTINCT location) AS n_locs,
            AVG(temperature) AS avg_temp,
            AVG(temp_max) AS avg_temp_max,
            AVG(temp_min) AS avg_temp_min,
            SUM(rainfall) AS total_rain,
            COUNT(CASE WHEN rainfall >= {RAINY_DAY_MM} THEN 1 END) AS rainy_days,
            AVG(humidity) AS avg_humidity,
            AVG(wind_speed) AS avg_wind_speed,
            AVG(pressure) AS avg_pressure
        FROM base
        WHERE year IN ({year_a}, {year_b})
        GROUP BY year
    ),
    metrics_unpivoted AS (
        SELECT
            'Average Temperature (°C)' AS metric,
            1 AS sort_order,
            MAX(CASE WHEN year = {year_a} THEN avg_temp END) AS val_a,
            MAX(CASE WHEN year = {year_b} THEN avg_temp END) AS val_b
        FROM year_stats
        UNION ALL
        SELECT
            'Average Maximum Temperature (°C)' AS metric,
            2 AS sort_order,
            MAX(CASE WHEN year = {year_a} THEN avg_temp_max END) AS val_a,
            MAX(CASE WHEN year = {year_b} THEN avg_temp_max END) AS val_b
        FROM year_stats
        UNION ALL
        SELECT
            'Average Minimum Temperature (°C)' AS metric,
            3 AS sort_order,
            MAX(CASE WHEN year = {year_a} THEN avg_temp_min END) AS val_a,
            MAX(CASE WHEN year = {year_b} THEN avg_temp_min END) AS val_b
        FROM year_stats
        UNION ALL
        SELECT
            'Total Rainfall per Location (mm)' AS metric,
            4 AS sort_order,
            MAX(CASE WHEN year = {year_a} THEN total_rain / NULLIF(n_locs, 0) END) AS val_a,
            MAX(CASE WHEN year = {year_b} THEN total_rain / NULLIF(n_locs, 0) END) AS val_b
        FROM year_stats
        UNION ALL
        SELECT
            'Rainy Days per Location (days)' AS metric,
            5 AS sort_order,
            MAX(CASE WHEN year = {year_a} THEN CAST(rainy_days AS DOUBLE) / NULLIF(n_locs, 0) END) AS val_a,
            MAX(CASE WHEN year = {year_b} THEN CAST(rainy_days AS DOUBLE) / NULLIF(n_locs, 0) END) AS val_b
        FROM year_stats
        UNION ALL
        SELECT
            'Average Relative Humidity (%)' AS metric,
            6 AS sort_order,
            MAX(CASE WHEN year = {year_a} THEN avg_humidity END) AS val_a,
            MAX(CASE WHEN year = {year_b} THEN avg_humidity END) AS val_b
        FROM year_stats
        UNION ALL
        SELECT
            'Average Wind Speed (km/h)' AS metric,
            7 AS sort_order,
            MAX(CASE WHEN year = {year_a} THEN avg_wind_speed END) AS val_a,
            MAX(CASE WHEN year = {year_b} THEN avg_wind_speed END) AS val_b
        FROM year_stats
        UNION ALL
        SELECT
            'Average Surface Pressure (hPa)' AS metric,
            8 AS sort_order,
            MAX(CASE WHEN year = {year_a} THEN avg_pressure END) AS val_a,
            MAX(CASE WHEN year = {year_b} THEN avg_pressure END) AS val_b
        FROM year_stats
    )
    SELECT
        metric,
        ROUND(val_a, 2) AS value_a,
        ROUND(val_b, 2) AS value_b,
        ROUND(val_b - val_a, 2) AS difference,
        CASE
            WHEN val_a IS NOT NULL AND val_a != 0.0
            THEN ROUND(((val_b - val_a) / val_a) * 100.0, 2)
            ELSE NULL
        END AS pct_change
    FROM metrics_unpivoted
    ORDER BY sort_order
    """
    return run_query(
        spark=spark,
        key="climate_year_comparison",
        title=f"Annual Meteorological Comparison ({year_a} vs {year_b})",
        description=f"Side-by-side metric comparison, absolute differences, and percentage deltas between {year_a} and {year_b}.",
        sql=sql,
        parameters={"year_a": year_a, "year_b": year_b, "filters": str(filters)},
    )


def climate_city_profile(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Comprehensive station meteorological profiles with cross-city rank benchmarks.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    ),
    loc_stats AS (
        SELECT
            location,
            ROUND(AVG(temperature), 2) AS avg_temp,
            ROUND(AVG(rainfall) * 365.25, 2) AS annual_rainfall_equiv,
            ROUND(AVG(humidity), 2) AS avg_humidity,
            ROUND(AVG(wind_speed), 2) AS avg_wind_speed,
            ROUND(AVG(pressure), 2) AS avg_pressure,
            ROUND(AVG(CASE WHEN rainfall >= {RAINY_DAY_MM} THEN 1.0 ELSE 0.0 END) * 365.25, 1) AS rainy_days_per_year,
            ROUND(MAX(temp_max), 2) AS record_max_temp,
            ROUND(MIN(temp_min), 2) AS record_min_temp,
            ROUND(MAX(rainfall), 2) AS record_rain
        FROM base
        GROUP BY location
    )
    SELECT
        location,
        avg_temp,
        annual_rainfall_equiv,
        avg_humidity,
        avg_wind_speed,
        avg_pressure,
        rainy_days_per_year,
        record_max_temp,
        record_min_temp,
        record_rain,
        RANK() OVER (ORDER BY avg_temp DESC, location ASC) AS temp_rank,
        RANK() OVER (ORDER BY annual_rainfall_equiv DESC, location ASC) AS rain_rank,
        RANK() OVER (ORDER BY avg_humidity DESC, location ASC) AS humidity_rank,
        RANK() OVER (ORDER BY avg_wind_speed DESC, location ASC) AS wind_rank,
        RANK() OVER (ORDER BY avg_pressure DESC, location ASC) AS pressure_rank
    FROM loc_stats
    ORDER BY location ASC
    """
    return run_query(
        spark=spark,
        key="climate_city_profile",
        title="Comprehensive Station Climate Profiles & Rankings",
        description="All-parameter summary profiles for each geographical center with relative ranks across stations.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def extreme_thresholds(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Historical station extreme event thresholds evaluated strictly on the FULL view.
    """
    # Important: Thresholds are ALWAYS calculated on the entire un-filtered view
    sql = f"""
    SELECT
        location,
        ROUND(percentile_approx(temp_max, {HOT_PERCENTILE}, {PERCENTILE_ACCURACY}), 2) AS hot_day_threshold,
        ROUND(percentile_approx(temp_min, {COLD_PERCENTILE}, {PERCENTILE_ACCURACY}), 2) AS cold_day_threshold,
        ROUND(percentile_approx(CASE WHEN rainfall >= {RAINY_DAY_MM} THEN rainfall END, {WET_DAY_EXTREME_PERCENTILE}, {PERCENTILE_ACCURACY}), 2) AS wet_day_extreme_threshold
    FROM {view}
    GROUP BY location
    ORDER BY location
    """
    return run_query(
        spark=spark,
        key="extreme_thresholds",
        title="Station Climatological Extreme Thresholds",
        description=f"95th percentile max temperature, 5th percentile min temperature, and 95th percentile wet-day rainfall calculated on the full dataset.",
        sql=sql,
        parameters={},
    )


def extreme_events_yearly(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Annual count of extreme weather days per location using full-view climatological thresholds.
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH full_thresholds AS (
        SELECT
            location,
            percentile_approx(temp_max, {HOT_PERCENTILE}, {PERCENTILE_ACCURACY}) AS hot_thresh,
            percentile_approx(temp_min, {COLD_PERCENTILE}, {PERCENTILE_ACCURACY}) AS cold_thresh,
            percentile_approx(CASE WHEN rainfall >= {RAINY_DAY_MM} THEN rainfall END, {WET_DAY_EXTREME_PERCENTILE}, {PERCENTILE_ACCURACY}) AS wet_thresh
        FROM {view}
        GROUP BY location
    ),
    base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    )
    SELECT
        b.location,
        b.year,
        COUNT(CASE WHEN b.temp_max >= t.hot_thresh THEN 1 END) AS hot_days,
        COUNT(CASE WHEN b.temp_min <= t.cold_thresh THEN 1 END) AS cold_days,
        COUNT(CASE WHEN b.rainfall >= t.wet_thresh AND b.rainfall >= {RAINY_DAY_MM} THEN 1 END) AS extreme_rain_days,
        COUNT(CASE WHEN b.rainfall >= {HEAVY_RAIN_MM} THEN 1 END) AS heavy_rain_days
    FROM base b
    JOIN full_thresholds t ON b.location = t.location
    GROUP BY b.location, b.year
    ORDER BY b.location, b.year
    """
    return run_query(
        spark=spark,
        key="extreme_events_yearly",
        title="Annual Extreme Events Frequency",
        description="Yearly occurrences of extreme heat, cold, and intense precipitation based on historical percentiles.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def extreme_top_events(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    n: int = TOP_N_DEFAULT,
    **params: Any,
) -> QueryResult:
    """
    Top historical extreme weather events (hottest days, coldest days, and wettest days).
    """
    where_clause = build_where(filters)
    sql = f"""
    WITH base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    ),
    hottest AS (
        SELECT
            'Hottest Day' AS category,
            location,
            CAST(date AS STRING) AS date,
            temp_max AS value,
            ROW_NUMBER() OVER (ORDER BY temp_max DESC, date ASC) AS rank
        FROM base
        WHERE temp_max IS NOT NULL
    ),
    coldest AS (
        SELECT
            'Coldest Day' AS category,
            location,
            CAST(date AS STRING) AS date,
            temp_min AS value,
            ROW_NUMBER() OVER (ORDER BY temp_min ASC, date ASC) AS rank
        FROM base
        WHERE temp_min IS NOT NULL
    ),
    wettest AS (
        SELECT
            'Wettest Day' AS category,
            location,
            CAST(date AS STRING) AS date,
            rainfall AS value,
            ROW_NUMBER() OVER (ORDER BY rainfall DESC, date ASC) AS rank
        FROM base
        WHERE rainfall IS NOT NULL
    ),
    unioned AS (
        SELECT category, rank, location, date, ROUND(value, 2) AS value FROM hottest WHERE rank <= {n}
        UNION ALL
        SELECT category, rank, location, date, ROUND(value, 2) AS value FROM coldest WHERE rank <= {n}
        UNION ALL
        SELECT category, rank, location, date, ROUND(value, 2) AS value FROM wettest WHERE rank <= {n}
    )
    SELECT category, rank, location, date, value
    FROM unioned
    ORDER BY category, rank
    """
    return run_query(
        spark=spark,
        key="extreme_top_events",
        title="Top Historical Weather Events",
        description=f"Top {n} all-time highest temperatures, lowest temperatures, and largest 24-hour rainfall totals.",
        sql=sql,
        parameters={"n": n, "filters": str(filters)},
    )


def heatwave_spells(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **params: Any,
) -> QueryResult:
    """
    Heatwave spell identification (>= 3 consecutive days above the 95th percentile)
    using the gaps-and-islands SQL technique.
    """
    where_clause = build_where(filters)
    # Thresholds are computed on the full view
    sql = f"""
    WITH full_thresholds AS (
        SELECT
            location,
            percentile_approx(temp_max, {HOT_PERCENTILE}, {PERCENTILE_ACCURACY}) AS hot_thresh
        FROM {view}
        GROUP BY location
    ),
    filtered_base AS (
        SELECT *
        FROM {view}
        WHERE {where_clause}
    ),
    flagged_days AS (
        SELECT
            b.location,
            b.date,
            b.temp_max,
            DATE_SUB(b.date, CAST(ROW_NUMBER() OVER (PARTITION BY b.location ORDER BY b.date) AS INT)) AS spell_grp
        FROM filtered_base b
        JOIN full_thresholds t ON b.location = t.location
        WHERE b.temp_max >= t.hot_thresh
    ),
    spells AS (
        SELECT
            location,
            MIN(date) AS spell_start,
            MAX(date) AS spell_end,
            COUNT(*) AS length_days,
            ROUND(MAX(temp_max), 2) AS peak_temp_max
        FROM flagged_days
        GROUP BY location, spell_grp
        HAVING COUNT(*) >= {HEATWAVE_MIN_DAYS}
    )
    SELECT
        location,
        CAST(spell_start AS STRING) AS spell_start,
        CAST(spell_end AS STRING) AS spell_end,
        length_days,
        peak_temp_max
    FROM spells
    ORDER BY length_days DESC, peak_temp_max DESC, location ASC
    """
    return run_query(
        spark=spark,
        key="heatwave_spells",
        title="Heatwave Spells (Gaps-and-Islands)",
        description=f"Periods of at least {HEATWAVE_MIN_DAYS} consecutive calendar days where daily maximum temperature reaches or exceeds the station 95th percentile.",
        sql=sql,
        parameters={"filters": str(filters)},
    )


def correlations(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    mode: str = "raw",
    scope: str = "all",
    **params: Any,
) -> QueryResult:
    """
    Pearson correlation analysis across all atmospheric variables.
    """
    where_clause = build_where(filters)
    var_list = CORRELATION_VARIABLES

    # Generate pairwise combinations
    pairs = []
    for i in range(len(var_list)):
        for j in range(i + 1, len(var_list)):
            pairs.append((var_list[i], var_list[j]))

    if mode == "anomaly":
        # Subtract monthly location mean to remove seasonal co-variation
        mean_cols = [f"AVG({v}) OVER (PARTITION BY location, month) AS `mean_{v}`" for v in var_list]
        anom_cols = [f"(`{v}` - `mean_{v}`) AS `{v}`" for v in var_list]
        base_cte = f"""
        base_raw AS (
            SELECT location, month, {', '.join([f'`{v}`' for v in var_list])},
                   {', '.join(mean_cols)}
            FROM {view}
            WHERE {where_clause}
        ),
        base AS (
            SELECT location, month, {', '.join(anom_cols)}
            FROM base_raw
        )
        """
    else:
        base_cte = f"""
        base AS (
            SELECT location, {', '.join([f'`{v}`' for v in var_list])}
            FROM {view}
            WHERE {where_clause}
        )
        """

    select_unions = []
    for x, y in pairs:
        if scope == "per_location":
            select_unions.append(f"""
            SELECT
                location,
                '{x}' AS var_x,
                '{y}' AS var_y,
                CORR(`{x}`, `{y}`) AS r,
                COUNT(CASE WHEN `{x}` IS NOT NULL AND `{y}` IS NOT NULL THEN 1 END) AS n_pairs
            FROM base
            GROUP BY location
            """)
        else:
            select_unions.append(f"""
            SELECT
                'All' AS location,
                '{x}' AS var_x,
                '{y}' AS var_y,
                CORR(`{x}`, `{y}`) AS r,
                COUNT(CASE WHEN `{x}` IS NOT NULL AND `{y}` IS NOT NULL THEN 1 END) AS n_pairs
            FROM base
            """)

    sql = f"""
    WITH {base_cte}
    { ' UNION ALL '.join(select_unions) }
    ORDER BY location, var_x, var_y
    """
    return run_query(
        spark=spark,
        key="correlations",
        title="Atmospheric Multi-Variate Correlation Matrix",
        description=f"Pairwise Pearson correlation coefficients ({mode} mode, {scope} scope).",
        sql=sql,
        parameters={"mode": mode, "scope": scope, "filters": str(filters)},
    )

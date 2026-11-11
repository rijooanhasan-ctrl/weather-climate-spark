"""
src/queries/registry.py
=======================
Central registry for all Spark SQL weather & climate analyses.
"""

from __future__ import annotations

from typing import Any, Callable

from pyspark.sql import SparkSession

from config.settings import CLEAN_VIEW_NAME
from src.queries.base import Filters, QueryResult
from src.queries.climate import (
    climate_city_profile,
    climate_year_comparison,
    climate_year_profile,
    correlations,
    extreme_events_yearly,
    extreme_thresholds,
    extreme_top_events,
    heatwave_spells,
)
from src.queries.kpis import kpi_summary
from src.queries.rainfall import (
    rain_heavy_days,
    rain_location_ranking,
    rain_monthly,
    rain_monthly_climatology,
    rain_seasonal,
    rain_unusual_months,
    rain_yearly,
)
from src.queries.temperature import (
    temp_location_ranking,
    temp_monthly_climatology,
    temp_monthly_series,
    temp_seasonal,
    temp_trend,
    temp_yearly,
)

ANALYSES: list[dict[str, Any]] = [
    # Temperature
    {
        "key": "temp_yearly",
        "category": "temperature",
        "title": "Yearly Temperature & Anomalies",
        "function": temp_yearly,
    },
    {
        "key": "temp_monthly_climatology",
        "category": "temperature",
        "title": "Monthly Temperature Climatology",
        "function": temp_monthly_climatology,
    },
    {
        "key": "temp_monthly_series",
        "category": "temperature",
        "title": "Monthly Temperature Series",
        "function": temp_monthly_series,
    },
    {
        "key": "temp_location_ranking",
        "category": "temperature",
        "title": "Location Temperature Ranking",
        "function": temp_location_ranking,
    },
    {
        "key": "temp_trend",
        "category": "temperature",
        "title": "Temperature Trend Analysis",
        "function": temp_trend,
    },
    {
        "key": "temp_seasonal",
        "category": "temperature",
        "title": "Seasonal Temperature Analysis",
        "function": temp_seasonal,
    },
    # Rainfall
    {
        "key": "rain_monthly",
        "category": "rainfall",
        "title": "Monthly Rainfall Totals",
        "function": rain_monthly,
    },
    {
        "key": "rain_yearly",
        "category": "rainfall",
        "title": "Yearly Rainfall & Rainy Days",
        "function": rain_yearly,
    },
    {
        "key": "rain_monthly_climatology",
        "category": "rainfall",
        "title": "Monthly Rainfall Climatology",
        "function": rain_monthly_climatology,
    },
    {
        "key": "rain_location_ranking",
        "category": "rainfall",
        "title": "Location Rainfall Ranking",
        "function": rain_location_ranking,
    },
    {
        "key": "rain_seasonal",
        "category": "rainfall",
        "title": "Seasonal Rainfall Distribution",
        "function": rain_seasonal,
    },
    {
        "key": "rain_unusual_months",
        "category": "rainfall",
        "title": "Unusual Rainfall Months (Z-Score)",
        "function": rain_unusual_months,
    },
    {
        "key": "rain_heavy_days",
        "category": "rainfall",
        "title": "Heavy Rainfall Frequency & Records",
        "function": rain_heavy_days,
    },
    # Climate
    {
        "key": "climate_year_profile",
        "category": "climate",
        "title": "Annual Climate Profile",
        "function": climate_year_profile,
    },
    {
        "key": "climate_year_comparison",
        "category": "climate",
        "title": "Climate Comparison Between Years",
        "function": climate_year_comparison,
    },
    {
        "key": "climate_city_profile",
        "category": "climate",
        "title": "Multi-City Climate Comparison",
        "function": climate_city_profile,
    },
    {
        "key": "extreme_thresholds",
        "category": "climate",
        "title": "Extreme Weather Thresholds",
        "function": extreme_thresholds,
    },
    {
        "key": "extreme_events_yearly",
        "category": "climate",
        "title": "Yearly Extreme Event Counts",
        "function": extreme_events_yearly,
    },
    {
        "key": "extreme_top_events",
        "category": "climate",
        "title": "Top Extreme Weather Events",
        "function": extreme_top_events,
    },
    {
        "key": "heatwave_spells",
        "category": "climate",
        "title": "Heatwave Spells (Gaps-and-Islands)",
        "function": heatwave_spells,
    },
    {
        "key": "correlations",
        "category": "climate",
        "title": "Climate Variable Correlations",
        "function": correlations,
    },
    # KPIs
    {
        "key": "kpi_summary",
        "category": "kpis",
        "title": "Executive Climate KPIs",
        "function": kpi_summary,
    },
]

ANALYSES_BY_KEY: dict[str, dict[str, Any]] = {
    item["key"]: item for item in ANALYSES
}


def get_analysis(key: str) -> dict[str, Any]:
    """
    Retrieve an analysis item by key or raise ValueError.
    """
    if key not in ANALYSES_BY_KEY:
        valid_keys = ", ".join(ANALYSES_BY_KEY.keys())
        raise ValueError(f"Unknown analysis key: '{key}'. Valid keys are: {valid_keys}")
    return ANALYSES_BY_KEY[key]


def run_all(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
    **kwargs: Any,
) -> dict[str, QueryResult]:
    """
    Execute all registered analyses against the specified clean view.
    """
    results: dict[str, QueryResult] = {}
    for item in ANALYSES:
        fn: Callable[..., QueryResult] = item["function"]
        results[item["key"]] = fn(spark, filters=filters, view=view, **kwargs)
    return results

"""
src/queries/__init__.py
=======================
Central export for Spark SQL weather and climate analysis queries.
"""

from __future__ import annotations

from src.queries.base import Filters, QueryResult, build_where, run_query
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
from src.queries.registry import ANALYSES, ANALYSES_BY_KEY, get_analysis, run_all
from src.queries.temperature import (
    temp_location_ranking,
    temp_monthly_climatology,
    temp_monthly_series,
    temp_seasonal,
    temp_trend,
    temp_yearly,
)

__all__ = [
    "Filters",
    "QueryResult",
    "build_where",
    "run_query",
    "ANALYSES",
    "ANALYSES_BY_KEY",
    "get_analysis",
    "run_all",
    "temp_yearly",
    "temp_monthly_climatology",
    "temp_monthly_series",
    "temp_location_ranking",
    "temp_trend",
    "temp_seasonal",
    "rain_monthly",
    "rain_yearly",
    "rain_monthly_climatology",
    "rain_location_ranking",
    "rain_seasonal",
    "rain_unusual_months",
    "rain_heavy_days",
    "climate_year_profile",
    "climate_year_comparison",
    "climate_city_profile",
    "extreme_thresholds",
    "extreme_events_yearly",
    "extreme_top_events",
    "heatwave_spells",
    "correlations",
    "kpi_summary",
]

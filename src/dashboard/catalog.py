"""
src/dashboard/catalog.py
========================
Catalog of dashboard sections and UI mapping for all 22 Spark SQL analysis queries.
Plain Python only - does NOT import streamlit.
"""

from typing import Any

SECTIONS: list[str] = [
    "Overview",
    "Temperature",
    "Rainfall",
    "Climate patterns",
    "Data quality",
    "Cleaning report",
    "Data preview",
    "Spark SQL",
]

ANALYSIS_UI_MAP: dict[str, dict[str, Any]] = {
    # ── Temperature Analyses (6) ─────────────────────────────────────────────
    "temp_yearly": {
        "section": "Temperature",
        "kind": "chart",
        "builder": "build_temp_yearly",
        "title": "Yearly Temperature & Anomalies",
        "description": "Annual mean temperature trends and baseline anomaly heatmaps by station.",
    },
    "temp_monthly_climatology": {
        "section": "Temperature",
        "kind": "chart",
        "builder": "build_temp_monthly_climatology",
        "title": "Monthly Temperature Climatology",
        "description": "Climatological 12-month annual thermal cycle curves comparing all stations.",
    },
    "temp_monthly_series": {
        "section": "Temperature",
        "kind": "chart",
        "builder": "build_temp_monthly_series",
        "title": "Monthly Temperature Series",
        "description": "Chronological monthly average temperature with 12-month moving baseline for a station.",
    },
    "temp_location_ranking": {
        "section": "Temperature",
        "kind": "chart",
        "builder": "build_temp_location_ranking",
        "title": "Location Temperature Ranking",
        "description": "Ranked station thermal comparison with all-time historical temperature extremes.",
    },
    "temp_trend": {
        "section": "Temperature",
        "kind": "chart+table",
        "builder": "build_temp_trend",
        "title": "Temperature Trend Analysis",
        "description": "Least-squares multi-decadal warming/cooling rates computed across complete years.",
    },
    "temp_seasonal": {
        "section": "Temperature",
        "kind": "chart",
        "builder": "build_temp_seasonal",
        "title": "Seasonal Temperature Analysis",
        "description": "Mean thermal levels grouped by meteorological season across all stations.",
    },

    # ── Rainfall Analyses (7) ────────────────────────────────────────────────
    "rain_yearly": {
        "section": "Rainfall",
        "kind": "chart",
        "builder": "build_rain_yearly",
        "title": "Yearly Rainfall & Rainy Days",
        "description": "Annual cumulative precipitation totals with incomplete years distinctly flagged.",
    },
    "rain_monthly": {
        "section": "Rainfall",
        "kind": "chart",
        "builder": "build_rain_monthly",
        "title": "Monthly Rainfall Totals",
        "description": "Year-by-month precipitation heatmap detailing seasonal monsoon cycles.",
    },
    "rain_monthly_climatology": {
        "section": "Rainfall",
        "kind": "chart",
        "builder": "build_rain_monthly_climatology",
        "title": "Monthly Rainfall Climatology",
        "description": "Average monthly precipitation regimes highlighting peak monsoon timing.",
    },
    "rain_location_ranking": {
        "section": "Rainfall",
        "kind": "chart",
        "builder": "build_rain_location_ranking",
        "title": "Location Rainfall Ranking",
        "description": "Comparative annual rainfall rankings with primary wettest calendar months.",
    },
    "rain_seasonal": {
        "section": "Rainfall",
        "kind": "chart",
        "builder": "build_rain_seasonal",
        "title": "Seasonal Rainfall Distribution",
        "description": "Monsoon share and percentage breakdown of annual rainfall by season.",
    },
    "rain_unusual_months": {
        "section": "Rainfall",
        "kind": "chart+table",
        "builder": "build_rain_unusual_months",
        "title": "Unusual Rainfall Months (Z-Score)",
        "description": "Precipitation anomalies exceeding statistical significance thresholds (Z >= 2.0).",
    },
    "rain_heavy_days": {
        "section": "Rainfall",
        "kind": "chart",
        "builder": "build_rain_heavy_days",
        "title": "Heavy Rainfall Frequency & Records",
        "description": "Annual frequency of heavy and extreme deluge events classified by IMD standards.",
    },

    # ── Climate Patterns & Extremes (8) ──────────────────────────────────────
    "climate_year_profile": {
        "section": "Climate patterns",
        "kind": "table",
        "builder": None,
        "title": "Annual Climate Profile",
        "description": "Consolidated multi-variable annual atmospheric profile across all observed years.",
    },
    "climate_year_comparison": {
        "section": "Climate patterns",
        "kind": "chart+table",
        "builder": "build_climate_year_comparison",
        "title": "Climate Comparison Between Years",
        "description": "Direct percentage shifts and anomalies comparing two chosen benchmark years.",
    },
    "climate_city_profile": {
        "section": "Climate patterns",
        "kind": "chart+table",
        "builder": "build_climate_city_profile",
        "title": "Multi-City Climate Comparison",
        "description": "Comprehensive comparative matrix and ranking across all meteorological variables.",
    },
    "extreme_thresholds": {
        "section": "Climate patterns",
        "kind": "table",
        "builder": None,
        "title": "Extreme Weather Thresholds",
        "description": "Station-specific statistical 95th/5th percentile thresholds (computed across full dataset).",
    },
    "extreme_events_yearly": {
        "section": "Climate patterns",
        "kind": "chart",
        "builder": "build_extreme_events_yearly",
        "title": "Yearly Extreme Event Counts",
        "description": "Annual counts of days breaching station extreme thresholds (hot, cold, heavy rain).",
    },
    "extreme_top_events": {
        "section": "Climate patterns",
        "kind": "table",
        "builder": None,
        "title": "Top Extreme Weather Events",
        "description": "Historical leaderboard of the most extreme temperature and precipitation observations.",
    },
    "heatwave_spells": {
        "section": "Climate patterns",
        "kind": "chart+table",
        "builder": "build_heatwave_spells",
        "title": "Heatwave Spells (Gaps-and-Islands)",
        "description": "Prolonged extreme heat episodes lasting 3 or more consecutive calendar days.",
    },
    "correlations": {
        "section": "Climate patterns",
        "kind": "chart",
        "builder": "build_correlations",
        "title": "Climate Variable Correlations",
        "description": "Pearson correlation matrix among primary atmospheric variables (raw and anomaly modes).",
    },

    # ── High-Level Overview (1) ──────────────────────────────────────────────
    "kpi_summary": {
        "section": "Overview",
        "kind": "table",
        "builder": None,
        "title": "Executive Climate KPIs",
        "description": "Top-level aggregate metrics powering the executive dashboard scorecard.",
    },
}


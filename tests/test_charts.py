"""
tests/test_charts.py
====================
Unit tests for Plotly visualization builders in src/dashboard/charts.py.
Verifies trace count, empty-state behavior, stable location colors,
and geometric properties of trendlines.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytest

from src.dashboard.charts import (
    build_climate_city_profile,
    build_climate_year_comparison,
    build_correlations,
    build_empty_figure,
    build_extreme_events_yearly,
    build_heatwave_spells,
    build_rain_heavy_days,
    build_rain_location_ranking,
    build_rain_monthly,
    build_rain_monthly_climatology,
    build_rain_seasonal,
    build_rain_unusual_months,
    build_rain_yearly,
    build_temp_location_ranking,
    build_temp_monthly_climatology,
    build_temp_monthly_series,
    build_temp_seasonal,
    build_temp_trend,
    build_temp_yearly,
    get_location_color,
)


def test_empty_dataframe_returns_empty_figure():
    """Empty DataFrames passed to builders must return an annotated empty figure without errors."""
    empty_df = pd.DataFrame()

    builders_single = [
        build_temp_yearly,
        build_temp_monthly_climatology,
        build_temp_monthly_series,
        build_temp_location_ranking,
        build_temp_seasonal,
        build_rain_yearly,
        build_rain_monthly,
        build_rain_monthly_climatology,
        build_rain_location_ranking,
        build_rain_seasonal,
        build_rain_heavy_days,
        build_climate_year_comparison,
        build_climate_city_profile,
        build_extreme_events_yearly,
        build_heatwave_spells,
        build_correlations,
    ]

    for builder in builders_single:
        fig = builder(empty_df)
        assert isinstance(fig, go.Figure)
        assert len(fig.layout.annotations) >= 1
        ann_text = fig.layout.annotations[0].text.lower()
        assert "no data" in ann_text or "no prolonged" in ann_text

    # Builders returning tuples (fig, table)
    builders_tuple = [
        build_temp_trend,
        build_rain_unusual_months,
    ]
    for builder in builders_tuple:
        fig, table = builder(empty_df)
        assert isinstance(fig, go.Figure)
        assert isinstance(table, pd.DataFrame)
        assert len(fig.layout.annotations) >= 1
        assert "no data" in fig.layout.annotations[0].text.lower()


def test_location_colors_are_stable():
    """Station colors must remain deterministic and stable regardless of filter subsets."""
    c_delhi_1 = get_location_color("Delhi")
    c_mumbai_1 = get_location_color("Mumbai")

    # Call repeatedly and verify exact identity
    c_delhi_2 = get_location_color("Delhi")
    c_mumbai_2 = get_location_color("Mumbai")
    assert c_delhi_1 == c_delhi_2
    assert c_mumbai_1 == c_mumbai_2
    assert c_delhi_1 != c_mumbai_1

    # Dynamic location
    c_custom_1 = get_location_color("Shimla")
    c_custom_2 = get_location_color("Shimla")
    assert c_custom_1 == c_custom_2


def test_temp_yearly_traces_and_values():
    """build_temp_yearly renders traces whose data matches the input DataFrame."""
    df = pd.DataFrame([
        {"location": "Delhi", "year": 2020, "avg_temp": 25.0, "anomaly_c": 0.5, "is_complete_year": True},
        {"location": "Delhi", "year": 2021, "avg_temp": 25.4, "anomaly_c": 0.9, "is_complete_year": True},
        {"location": "Delhi", "year": 2022, "avg_temp": 24.8, "anomaly_c": 0.3, "is_complete_year": True},
    ])
    fig = build_temp_yearly(df)
    assert isinstance(fig, go.Figure)
    assert len(fig.data) >= 1

    # First trace is annual mean temperature scatter
    scatter_trace = fig.data[0]
    np.testing.assert_array_equal(scatter_trace.x, [2020, 2021, 2022])
    np.testing.assert_array_almost_equal(scatter_trace.y, [25.0, 25.4, 24.8])


def test_rain_yearly_incomplete_year_trace():
    """build_rain_yearly must render complete bars and distinct incomplete year markers."""
    df = pd.DataFrame([
        {"location": "Mumbai", "year": 2020, "total_rain": 2400.0, "rainy_days": 80, "is_complete_year": True},
        {"location": "Mumbai", "year": 2021, "total_rain": 2600.0, "rainy_days": 85, "is_complete_year": True},
        {"location": "Mumbai", "year": 2022, "total_rain": 1200.0, "rainy_days": 40, "is_complete_year": False},
        {"location": "Mumbai", "year": 2023, "total_rain": 900.0, "rainy_days": 30, "is_complete_year": False},
    ])
    fig = build_rain_yearly(df)
    assert isinstance(fig, go.Figure)
    assert len(fig.data) >= 2  # Complete bars + Incomplete markers

    # Verify incomplete markers trace
    incomplete_trace = [t for t in fig.data if "Incomplete" in t.name]
    assert len(incomplete_trace) == 1
    assert len(incomplete_trace[0].x) == 2  # exactly 2 incomplete years (2022, 2023)
    np.testing.assert_array_equal(incomplete_trace[0].x, [2022, 2023])


def test_trend_line_geometry():
    """
    build_temp_trend must render a trendline passing through (mean_x, mean_y)
    with slope matching Spark SQL regr_slope.
    """
    trend_df = pd.DataFrame([
        {
            "location": "Delhi",
            "trend_label": "warming",
            "slope_c_per_year": 0.03,
            "slope_per_decade": 0.30,
            "mean_temp_all_years": 25.0,
            "min_year": 2010,
            "max_year": 2020,
            "years_count": 11,
            "is_significant": True,
        }
    ])
    yearly_df = pd.DataFrame([
        {"location": "Delhi", "year": y, "avg_temp": 25.0 + 0.03 * (y - 2015), "is_complete_year": True}
        for y in range(2010, 2021)
    ])

    fig, stats = build_temp_trend(trend_df, yearly_points_df=yearly_df)
    assert isinstance(fig, go.Figure)
    assert len(fig.data) >= 2

    # Find the trend line trace
    line_traces = [t for t in fig.data if t.mode == "lines" and "trend" in t.name.lower()]
    assert len(line_traces) == 1
    trend_trace = line_traces[0]

    # Verify line endpoints
    x_coords = list(trend_trace.x)
    y_coords = list(trend_trace.y)
    assert x_coords[0] == 2010
    assert x_coords[-1] == 2020

    # Slope is 0.30 °C/decade => 0.03 °C/year
    # Across 10 years (2010 to 2020), total change = 0.30 °C
    total_change = y_coords[-1] - y_coords[0]
    assert pytest.approx(total_change, 0.01) == 0.30

    # Centroid check: at midpoint year (2015), y must equal mean_temp (25.0)
    mid_y = (y_coords[0] + y_coords[-1]) / 2.0
    assert pytest.approx(mid_y, 0.01) == 25.0


def test_all_builders_with_valid_fixtures():
    """Verify that every builder executes and produces at least one trace given representative data."""
    # 1. temp_monthly_climatology
    clim_df = pd.DataFrame([
        {"location": "Delhi", "month": m, "month_name": f"M{m}", "avg_temp": 20.0 + m}
        for m in range(1, 13)
    ])
    assert len(build_temp_monthly_climatology(clim_df).data) >= 1

    # 2. temp_monthly_series
    series_df = pd.DataFrame([
        {"location": "Delhi", "year": 2020, "month": m, "avg_temp": 22.0 + m, "rolling_avg_12m": 25.0}
        for m in range(1, 13)
    ])
    assert len(build_temp_monthly_series(series_df, location="Delhi").data) >= 1

    # 3. temp_location_ranking
    rank_df = pd.DataFrame([
        {"location": "Delhi", "avg_temp": 25.0, "rank_hot_to_cold": 1, "all_time_max": 48.0, "all_time_min": 2.0}
    ])
    assert len(build_temp_location_ranking(rank_df).data) >= 1

    # 4. temp_seasonal
    seas_df = pd.DataFrame([
        {"location": "Delhi", "season": "Summer", "avg_temp": 32.0, "min_temp": 25.0, "max_temp": 40.0}
    ])
    assert len(build_temp_seasonal(seas_df).data) >= 1

    # 5. rain_monthly
    rain_m_df = pd.DataFrame([
        {"location": "Delhi", "year": 2020, "month": m, "total_rain": 50.0}
        for m in range(1, 13)
    ])
    assert len(build_rain_monthly(rain_m_df, location="Delhi").data) >= 1

    # 6. rain_monthly_climatology
    rain_clim_df = pd.DataFrame([
        {"location": "Delhi", "month": m, "month_name": f"M{m}", "avg_monthly_total": 60.0}
        for m in range(1, 13)
    ])
    assert len(build_rain_monthly_climatology(rain_clim_df).data) >= 1

    # 7. rain_location_ranking
    rain_rank_df = pd.DataFrame([
        {"location": "Delhi", "total_rain": 8000.0, "avg_annual_rain": 800.0, "wettest_month": 7}
    ])
    assert len(build_rain_location_ranking(rain_rank_df).data) >= 1

    # 8. rain_seasonal
    rain_seas_df = pd.DataFrame([
        {"location": "Delhi", "season": "Monsoon", "total_rain": 600.0, "share_of_annual_pct": 75.0}
    ])
    assert len(build_rain_seasonal(rain_seas_df).data) >= 1

    # 9. rain_unusual_months
    unusual_df = pd.DataFrame([
        {"location": "Delhi", "year": 2020, "month": 7, "month_name": "July", "total_rain": 350.0, "climatological_mean_mm": 150.0, "z_score": 2.5, "is_unusually_high": True}
    ])
    fig_u, _ = build_rain_unusual_months(unusual_df, location="Delhi")
    assert len(fig_u.data) >= 1

    # 10. rain_heavy_days
    heavy_df = pd.DataFrame([
        {"location": "Delhi", "year": 2020, "heavy_rain_days": 3, "very_heavy_rain_days": 1, "extreme_rain_days": 2}
    ])
    assert len(build_rain_heavy_days(heavy_df, location="Delhi").data) >= 1

    # 11. climate_year_comparison
    cmp_df = pd.DataFrame([
        {"metric": "avg_temp", "pct_change": 4.0}
    ])
    assert len(build_climate_year_comparison(cmp_df).data) >= 1

    # 12. climate_city_profile
    cprof_df = pd.DataFrame([
        {"location": "Delhi", "avg_temp": 25.0, "annual_rainfall_equiv": 800.0, "avg_humidity": 60.0}
    ])
    assert len(build_climate_city_profile(cprof_df).data) >= 1

    # 13. extreme_events_yearly
    ev_df = pd.DataFrame([
        {"location": "Delhi", "year": 2020, "hot_days": 15, "cold_days": 10, "extreme_rain_days": 2, "heavy_rain_days": 4}
    ])
    assert len(build_extreme_events_yearly(ev_df).data) >= 1

    # 14. heatwave_spells
    hw_df = pd.DataFrame([
        {"location": "Delhi", "spell_start": "2024-05-15", "spell_end": "2024-05-20", "length_days": 6, "peak_temp_max": 46.5}
    ])
    fig_hw = build_heatwave_spells(hw_df)
    assert len(fig_hw.data) >= 1

    # 15. correlations
    corr_df = pd.DataFrame([
        {"location": "All locations", "var_x": "temp_max", "var_y": "temperature", "r": 0.85, "n_pairs": 46000},
        {"location": "All locations", "var_x": "temperature", "var_y": "humidity", "r": -0.60, "n_pairs": 46000},
    ])
    assert len(build_correlations(corr_df).data) >= 1

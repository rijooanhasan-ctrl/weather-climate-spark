"""
src/dashboard/charts.py
=======================
Plotly visualization builders for all Spark SQL weather & climate analyses.
Pure Python and pandas - does NOT import streamlit.

PROJECT RULES ENFORCED:
- Zero recomputation of analytical metrics in Pandas or NumPy.
- All metrics, anomalies, slopes, percentiles, and counts were computed by Apache Spark SQL.
- Pure display, formatting, pivoting, and layout only.
- Strict empty-state safety: empty DataFrames return an annotated empty figure and never raise.
- Stable location colors across filter changes.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config import settings
from config.schema import UNITS
import src.dashboard.theme  # Registers "weather_warm" Plotly template

# ── Color Palettes & Location Mapping ─────────────────────────────────────────

# Canonical sorted list of primary Indian stations for stable color indexing
CANONICAL_LOCATIONS: list[str] = [
    "Bengaluru",
    "Chennai",
    "Delhi",
    "Hyderabad",
    "Jaipur",
    "Kolkata",
    "Mumbai",
    "Shillong",
]

LOCATION_COLOR_MAP: dict[str, str] = {
    loc: settings.LOCATION_PALETTE[i % len(settings.LOCATION_PALETTE)]
    for i, loc in enumerate(CANONICAL_LOCATIONS)
}


def get_location_color(loc: str) -> str:
    """Return a deterministic, stable color for a location name from LOCATION_PALETTE."""
    if loc in LOCATION_COLOR_MAP:
        return LOCATION_COLOR_MAP[loc]
    # For any uploaded dynamic location: hash-based deterministic selection from LOCATION_PALETTE
    idx = abs(hash(loc)) % len(settings.LOCATION_PALETTE)
    return settings.LOCATION_PALETTE[idx]


def build_empty_figure(annotation_text: str = "No data for the current selection") -> go.Figure:
    """Return an empty warm-themed figure with a centered informative annotation."""
    fig = go.Figure()
    fig.add_annotation(
        text=annotation_text,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font=dict(family="Plus Jakarta Sans, sans-serif", size=14, color="#7A6E65"),
    )
    fig.update_layout(
        template=settings.PLOTLY_TEMPLATE_NAME,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=settings.CHART_HEIGHT,
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        margin=dict(l=40, r=20, t=50, b=40),
    )
    return fig


def apply_theme(fig: go.Figure, title: str | None = None, height: int | None = None) -> go.Figure:
    """Apply consistent styling, transparent backgrounds, and typography."""
    layout_update: dict[str, Any] = {
        "template": settings.PLOTLY_TEMPLATE_NAME,
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "height": height or settings.CHART_HEIGHT,
        "font": dict(family="Plus Jakarta Sans, sans-serif", color="#1F1815"),
        "margin": dict(l=50, r=40, t=50, b=50),
    }
    if title:
        layout_update["title"] = dict(
            text=title,
            font=dict(family="Epilogue, sans-serif", size=15, color="#1F1815"),
        )
    fig.update_layout(**layout_update)
    if not title:
        if hasattr(fig.layout, "title") and not getattr(fig.layout.title, "text", None):
            fig.layout.pop("title", None)
    return fig


def _check_location_limit(fig: go.Figure, locations: list[str]) -> list[str]:
    """Annotate figure if locations exceed MAX_LOCATIONS_IN_CHART and slice list."""
    if len(locations) > settings.MAX_LOCATIONS_IN_CHART:
        fig.add_annotation(
            text=f"Showing {settings.MAX_LOCATIONS_IN_CHART} of {len(locations)} locations",
            xref="paper",
            yref="paper",
            x=1.0,
            y=1.05,
            showarrow=False,
            font=dict(size=11, color="#F59E0B"),
            xanchor="right",
        )
        return locations[: settings.MAX_LOCATIONS_IN_CHART]
    return locations


# =============================================================================
# 1. TEMPERATURE VISUALIZATION BUILDERS
# =============================================================================

def build_temp_yearly(df: pd.DataFrame) -> go.Figure:
    """
    Build annual mean temperature trends and baseline anomaly heatmap.
    Uses subplots: Top = Multi-station temperature trend lines; Bottom = Anomaly Heatmap.
    """
    if df.empty or "location" not in df.columns:
        return build_empty_figure()

    fig = make_subplots(
        rows=2,
        cols=1,
        subplot_titles=[
            f"Annual Mean Temperature Trends ({UNITS.get('temperature', '°C')})",
            f"Temperature Anomaly vs Station Mean Baseline ({UNITS.get('temperature', '°C')})",
        ],
        vertical_spacing=0.18,
    )

    all_locs = sorted(df["location"].unique())
    locs_to_show = _check_location_limit(fig, all_locs)

    # 1. Top Subplot: Lines per location
    has_incomplete = False
    for loc in locs_to_show:
        loc_df = df[df["location"] == loc].sort_values("year")
        color = get_location_color(loc)
        fig.add_trace(
            go.Scatter(
                x=loc_df["year"],
                y=loc_df["avg_temp"],
                mode="lines+markers",
                name=loc,
                line=dict(color=color, width=2),
                marker=dict(size=5),
                hovertemplate=f"<b>{loc}</b><br>Year: %{{x}}<br>Avg Temp: %{{y:.2f}} °C<extra></extra>",
            ),
            row=1,
            col=1,
        )
        # Check for incomplete years in this station
        if "is_complete_year" in loc_df.columns:
            incomp = loc_df[loc_df["is_complete_year"] == False]
            if not incomp.empty:
                has_incomplete = True
                fig.add_trace(
                    go.Scatter(
                        x=incomp["year"],
                        y=incomp["avg_temp"],
                        mode="markers",
                        name="Incomplete year (excluded from trends)" if loc == locs_to_show[0] else None,
                        showlegend=(loc == locs_to_show[0]),
                        marker=dict(symbol="circle-open", size=9, color="#EF4444", line=dict(width=2)),
                        hovertemplate=f"<b>{loc} (Incomplete)</b><br>Year: %{{x}}<br>Avg Temp: %{{y:.2f}} °C<extra></extra>",
                    ),
                    row=1,
                    col=1,
                )

    # 2. Bottom Subplot: Location x Year Anomaly Heatmap
    if "anomaly_vs_location_mean" in df.columns:
        pivoted = df[df["location"].isin(locs_to_show)].pivot(
            index="location", columns="year", values="anomaly_vs_location_mean"
        )
        pivoted = pivoted.reindex(sorted(pivoted.index))
        years = list(pivoted.columns)
        locations = list(pivoted.index)
        z_vals = pivoted.to_numpy()

        # Compute symmetric max for zero-centered diverging colorscale
        max_abs = float(np.nanmax(np.abs(z_vals))) if z_vals.size > 0 else 2.0
        max_abs = max(max_abs, 1.0)

        fig.add_trace(
            go.Heatmap(
                z=z_vals,
                x=years,
                y=locations,
                colorscale=settings.DIVERGING_COLORSCALE,
                zmid=0,
                zmin=-max_abs,
                zmax=max_abs,
                colorbar=dict(title="Anomaly (°C)", y=0.22, len=0.45),
                hovertemplate="Station: %{y}<br>Year: %{x}<br>Anomaly: %{z:+.2f} °C<extra></extra>",
            ),
            row=2,
            col=1,
        )

    fig.update_xaxes(title_text="Year", row=1, col=1)
    fig.update_yaxes(title_text=f"Temperature ({UNITS.get('temperature', '°C')})", row=1, col=1)
    fig.update_xaxes(title_text="Year", row=2, col=1)
    fig.update_yaxes(title_text="Station", row=2, col=1)

    return apply_theme(fig, height=settings.CHART_HEIGHT_TALL)


def build_temp_monthly_climatology(df: pd.DataFrame) -> go.Figure:
    """Climatological 12-month temperature cycle per station."""
    if df.empty or "location" not in df.columns:
        return build_empty_figure()

    fig = go.Figure()
    all_locs = sorted(df["location"].unique())
    locs_to_show = _check_location_limit(fig, all_locs)

    for loc in locs_to_show:
        loc_df = df[df["location"] == loc].sort_values("month")
        fig.add_trace(
            go.Scatter(
                x=loc_df["month"],
                y=loc_df["avg_temp"],
                mode="lines+markers",
                name=loc,
                line=dict(color=get_location_color(loc), width=2),
                marker=dict(size=6),
                hovertemplate=f"<b>{loc}</b><br>Month: %{{x}}<br>Mean Temp: %{{y:.1f}} °C<extra></extra>",
            )
        )

    fig.update_layout(
        xaxis=dict(
            tickmode="array",
            tickvals=list(range(1, 13)),
            ticktext=settings.MONTH_NAMES,
            title="Month",
        ),
        yaxis=dict(title=f"Climatological Mean Temperature ({UNITS.get('temperature', '°C')})"),
    )
    return apply_theme(fig, title="Monthly Temperature Climatology (12-Month Station Regimes)")


def build_temp_monthly_series(df: pd.DataFrame, location: str | None = None) -> go.Figure:
    """Chronological monthly mean temperature series with Spark-computed 12m rolling avg for one station."""
    if df.empty:
        return build_empty_figure()

    locs = sorted(df["location"].unique()) if "location" in df.columns else []
    target_loc = location if (location and location in locs) else (locs[0] if locs else None)

    loc_df = df[df["location"] == target_loc].copy() if target_loc else df.copy()
    if loc_df.empty:
        return build_empty_figure(f"No series data for station '{target_loc}'")

    loc_df["date_str"] = (
        loc_df["year"].astype(str) + "-" + loc_df["month"].astype(str).str.zfill(2)
    )
    loc_df = loc_df.sort_values(["year", "month"])

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=loc_df["date_str"],
            y=loc_df["avg_temp"],
            mode="lines",
            name="Monthly Mean",
            line=dict(color="#38BDF8", width=1.5),
            hovertemplate="Month: %{x}<br>Mean: %{y:.1f} °C<extra></extra>",
        )
    )

    if "rolling_12m_avg" in loc_df.columns:
        fig.add_trace(
            go.Scatter(
                x=loc_df["date_str"],
                y=loc_df["rolling_12m_avg"],
                mode="lines",
                name="12-Month Moving Average (Spark SQL)",
                line=dict(color="#F59E0B", width=2.5),
                hovertemplate="Month: %{x}<br>12M Avg: %{y:.2f} °C<extra></extra>",
            )
        )

    fig.update_layout(
        xaxis=dict(title="Year-Month", tickangle=-45, nticks=20),
        yaxis=dict(title=f"Temperature ({UNITS.get('temperature', '°C')})"),
    )
    title = f"Chronological Temperature Series — {target_loc}" if target_loc else "Chronological Temperature Series"
    return apply_theme(fig, title=title)


def build_temp_location_ranking(df: pd.DataFrame) -> go.Figure:
    """Sorted horizontal bar comparison of station average temperature with extremes in hover."""
    if df.empty or "location" not in df.columns or "avg_temp" not in df.columns:
        return build_empty_figure()

    sorted_df = df.sort_values("avg_temp", ascending=True)

    hover_texts = []
    for _, r in sorted_df.iterrows():
        txt = (
            f"<b>{r['location']}</b><br>"
            f"Average Temperature: {r['avg_temp']:.2f} °C<br>"
            f"Record Max: {r.get('record_max', 'n/a')} °C ({r.get('record_max_date', '')})<br>"
            f"Record Min: {r.get('record_min', 'n/a')} °C ({r.get('record_min_date', '')})"
        )
        hover_texts.append(txt)

    colors = [get_location_color(loc) for loc in sorted_df["location"]]

    fig = go.Figure(
        go.Bar(
            y=sorted_df["location"],
            x=sorted_df["avg_temp"],
            orientation="h",
            marker=dict(color=colors),
            hovertext=hover_texts,
            hoverinfo="text",
        )
    )
    fig.update_layout(
        xaxis=dict(title=f"Historical Average Temperature ({UNITS.get('temperature', '°C')})"),
        yaxis=dict(title="Station"),
    )
    return apply_theme(fig, title="Station Temperature Ranking (Ascending Thermal Levels)")


def build_temp_trend(df: pd.DataFrame, yearly_points_df: pd.DataFrame | None = None) -> tuple[go.Figure, pd.DataFrame]:
    """
    Least-squares multi-decadal temperature trend visualization and statistics table.
    
    NOTE ON TREND LINE DRAWING:
    The regression slope is taken strictly from the Spark SQL result (`slope_per_decade / 10`).
    The line is drawn across plotted points as: y = mean_y + slope * (x - mean_x),
    where mean_x and mean_y are the coordinate centroids of the plotted points for drawing only.
    No regression or slope math is computed in Python/Pandas.
    """
    if df.empty or "location" not in df.columns:
        return build_empty_figure(), pd.DataFrame()

    fig = go.Figure()
    all_locs = [l for l in sorted(df["location"].unique()) if l != "All locations"]
    locs_to_show = _check_location_limit(fig, all_locs)

    # If yearly points dataframe is provided (or pivoted), plot them
    # Otherwise render bar comparison of slopes per decade
    has_points = (
        yearly_points_df is not None
        and not yearly_points_df.empty
        and "year" in yearly_points_df.columns
        and "avg_temp" in yearly_points_df.columns
    )

    if has_points:
        for loc in locs_to_show:
            loc_pts = yearly_points_df[yearly_points_df["location"] == loc].sort_values("year")
            if loc_pts.empty:
                continue
            color = get_location_color(loc)
            fig.add_trace(
                go.Scatter(
                    x=loc_pts["year"],
                    y=loc_pts["avg_temp"],
                    mode="markers",
                    name=loc,
                    marker=dict(color=color, size=6),
                    hovertemplate=f"<b>{loc}</b><br>Year: %{{x}}<br>Mean: %{{y:.2f}} °C<extra></extra>",
                )
            )

            # Draw trend line using Spark slope if valid
            loc_stats = df[df["location"] == loc]
            if not loc_stats.empty:
                r = loc_stats.iloc[0]
                label = r.get("trend_label", "")
                if label not in ("insufficient data", "no clear trend"):
                    slope = float(r.get("slope_c_per_year", 0.0))
                    xs = loc_pts["year"].to_numpy().astype(float)
                    ys = loc_pts["avg_temp"].to_numpy().astype(float)
                    mean_x = float(np.mean(xs))
                    mean_y = float(np.mean(ys))
                    line_y = mean_y + slope * (xs - mean_x)
                    fig.add_trace(
                        go.Scatter(
                            x=xs,
                            y=line_y,
                            mode="lines",
                            name=f"{loc} trend ({r.get('slope_per_decade', 0.0):+.2f} °C/dec)",
                            line=dict(color=color, dash="dash", width=1.8),
                            hoverinfo="skip",
                        )
                    )
        fig.update_layout(
            xaxis=dict(title="Year"),
            yaxis=dict(title=f"Annual Mean Temperature ({UNITS.get('temperature', '°C')})"),
        )
    else:
        # Fallback view: Bar comparison of slope per decade across stations
        sorted_df = df[df["location"].isin(locs_to_show)].sort_values("slope_per_decade")
        colors = ["#EF4444" if s > 0 else "#38BDF8" for s in sorted_df["slope_per_decade"]]
        fig.add_trace(
            go.Bar(
                x=sorted_df["location"],
                y=sorted_df["slope_per_decade"],
                marker=dict(color=colors),
                hovertemplate="Station: %{x}<br>Slope: %{y:+.3f} °C/decade<extra></extra>",
            )
        )
        fig.update_layout(
            xaxis=dict(title="Station"),
            yaxis=dict(title="Warming / Cooling Rate (°C / decade)"),
        )

    apply_theme(fig, title="Long-Term Temperature Trends (Spark SQL OLS Regression)")
    return fig, df


def build_temp_seasonal(df: pd.DataFrame) -> go.Figure:
    """Grouped bars comparing seasonal temperatures across stations in SEASON_ORDER."""
    if df.empty or "location" not in df.columns or "season" not in df.columns:
        return build_empty_figure()

    fig = go.Figure()
    all_locs = sorted(df["location"].unique())
    locs_to_show = _check_location_limit(fig, all_locs)

    season_colors = {
        "Winter": "#38BDF8",
        "Summer": "#F59E0B",
        "Summer or Pre-monsoon": "#F59E0B",
        "Monsoon": "#10B981",
        "Post-monsoon": "#8B5CF6",
    }

    # Present seasons strictly according to SEASON_ORDER
    available_seasons = [s for s in settings.SEASON_ORDER if s in df["season"].unique()]

    for season in available_seasons:
        s_df = df[(df["season"] == season) & (df["location"].isin(locs_to_show))]
        # Order by station
        s_df = s_df.set_index("location").reindex(locs_to_show).reset_index()
        fig.add_trace(
            go.Bar(
                x=s_df["location"],
                y=s_df["avg_temp"],
                name=season,
                marker=dict(color=season_colors.get(season, "#94A3B8")),
                hovertemplate=f"<b>{season}</b><br>Station: %{{x}}<br>Mean: %{{y:.1f}} °C<extra></extra>",
            )
        )

    fig.update_layout(
        barmode="group",
        xaxis=dict(title="Station"),
        yaxis=dict(title=f"Mean Temperature ({UNITS.get('temperature', '°C')})"),
    )
    return apply_theme(fig, title="Seasonal Temperature Patterns (Grouped by Meteorological Season)")


# =============================================================================
# 2. RAINFALL VISUALIZATION BUILDERS
# =============================================================================

def build_rain_yearly(df: pd.DataFrame) -> go.Figure:
    """Grouped annual cumulative precipitation bars per year with incomplete years distinctly marked."""
    if df.empty or "location" not in df.columns:
        return build_empty_figure()

    fig = go.Figure()
    all_locs = sorted(df["location"].unique())
    locs_to_show = _check_location_limit(fig, all_locs)

    for loc in locs_to_show:
        loc_df = df[df["location"] == loc].sort_values("year")
        color = get_location_color(loc)
        fig.add_trace(
            go.Bar(
                x=loc_df["year"],
                y=loc_df["total_rain"],
                name=loc,
                marker=dict(color=color),
                hovertemplate=f"<b>{loc}</b><br>Year: %{{x}}<br>Total Rain: %{{y:,.1f}} mm<extra></extra>",
            )
        )

    # Add marker trace for incomplete years
    if "is_complete_year" in df.columns:
        incomp = df[(df["is_complete_year"] == False) & (df["location"].isin(locs_to_show))]
        if not incomp.empty:
            fig.add_trace(
                go.Scatter(
                    x=incomp["year"],
                    y=incomp["total_rain"],
                    mode="markers",
                    name="Incomplete year (excluded from trends)",
                    marker=dict(symbol="x-thin", size=10, color="#EF4444", line=dict(width=2)),
                    hovertemplate="<b>Incomplete Year</b><br>Year: %{x}<br>Rain: %{y:,.1f} mm<extra></extra>",
                )
            )

    fig.update_layout(
        barmode="group",
        xaxis=dict(title="Year"),
        yaxis=dict(title=f"Annual Cumulative Rainfall ({UNITS.get('rainfall', 'mm')})"),
    )
    return apply_theme(fig, title="Annual Cumulative Precipitation & Completeness")


def build_rain_monthly(df: pd.DataFrame, location: str | None = None) -> go.Figure:
    """Year x Month precipitation heatmap for ONE selected location."""
    if df.empty:
        return build_empty_figure()

    locs = sorted(df["location"].unique()) if "location" in df.columns else []
    target_loc = location if (location and location in locs) else (locs[0] if locs else None)

    loc_df = df[df["location"] == target_loc] if target_loc else df
    if loc_df.empty:
        return build_empty_figure(f"No rainfall records for station '{target_loc}'")

    pivoted = loc_df.pivot(index="year", columns="month", values="total_rain")
    years = list(pivoted.index)
    month_nums = list(pivoted.columns)
    month_labels = [
        settings.MONTH_NAMES[m - 1] if (1 <= m <= 12) else str(m) for m in month_nums
    ]
    z_vals = pivoted.to_numpy()

    fig = go.Figure(
        go.Heatmap(
            z=z_vals,
            x=month_labels,
            y=years,
            colorscale=settings.SEQUENTIAL_COLORSCALE,
            colorbar=dict(title=f"Rain ({UNITS.get('rainfall', 'mm')})"),
            hovertemplate="Year: %{y}<br>Month: %{x}<br>Total Rain: %{z:,.1f} mm<extra></extra>",
        )
    )
    fig.update_layout(
        xaxis=dict(title="Month"),
        yaxis=dict(title="Year", autorange="reversed"),
    )
    title = f"Monthly Precipitation Heatmap — {target_loc}" if target_loc else "Monthly Precipitation Heatmap"
    return apply_theme(fig, title=title)


def build_rain_monthly_climatology(df: pd.DataFrame) -> go.Figure:
    """Climatological monthly rainfall curves across all stations."""
    if df.empty or "location" not in df.columns:
        return build_empty_figure()

    fig = go.Figure()
    all_locs = sorted(df["location"].unique())
    locs_to_show = _check_location_limit(fig, all_locs)

    for loc in locs_to_show:
        loc_df = df[df["location"] == loc].sort_values("month")
        fig.add_trace(
            go.Scatter(
                x=loc_df["month"],
                y=loc_df["avg_monthly_total"],
                mode="lines+markers",
                name=loc,
                line=dict(color=get_location_color(loc), width=2),
                marker=dict(size=5),
                hovertemplate=f"<b>{loc}</b><br>Month: %{{x}}<br>Mean Rain: %{{y:,.1f}} mm<extra></extra>",
            )
        )

    fig.update_layout(
        xaxis=dict(
            tickmode="array",
            tickvals=list(range(1, 13)),
            ticktext=settings.MONTH_NAMES,
            title="Month",
        ),
        yaxis=dict(title=f"Mean Monthly Rainfall ({UNITS.get('rainfall', 'mm')})"),
    )
    return apply_theme(fig, title="Monthly Rainfall Climatology (Monsoon Seasonality)")


def build_rain_location_ranking(df: pd.DataFrame) -> go.Figure:
    """Sorted horizontal bars of annual rainfall with wettest month and complete years in hover."""
    if df.empty or "location" not in df.columns or "avg_annual_rain" not in df.columns:
        return build_empty_figure()

    sorted_df = df.sort_values("avg_annual_rain", ascending=True)

    hover_texts = []
    for _, r in sorted_df.iterrows():
        w_m = int(r.get("wettest_month", 0))
        w_name = settings.MONTH_NAMES[w_m - 1] if 1 <= w_m <= 12 else str(w_m)
        txt = (
            f"<b>{r['location']}</b><br>"
            f"Annual Average: {r['avg_annual_rain']:,.1f} mm<br>"
            f"Wettest Month: {w_name}<br>"
            f"Complete Years Used: {r.get('complete_years_used', 'all')}"
        )
        hover_texts.append(txt)

    colors = [get_location_color(loc) for loc in sorted_df["location"]]

    fig = go.Figure(
        go.Bar(
            y=sorted_df["location"],
            x=sorted_df["avg_annual_rain"],
            orientation="h",
            marker=dict(color=colors),
            hovertext=hover_texts,
            hoverinfo="text",
        )
    )
    fig.update_layout(
        xaxis=dict(title=f"Average Annual Rainfall ({UNITS.get('rainfall', 'mm')})"),
        yaxis=dict(title="Station"),
    )
    return apply_theme(fig, title="Station Annual Rainfall Ranking (Ascending Precipitation)")


def build_rain_seasonal(df: pd.DataFrame) -> go.Figure:
    """Stacked bars of seasonal rainfall share (share_of_annual_pct) per station in SEASON_ORDER."""
    if df.empty or "location" not in df.columns or "season" not in df.columns:
        return build_empty_figure()

    fig = go.Figure()
    all_locs = sorted(df["location"].unique())
    locs_to_show = _check_location_limit(fig, all_locs)

    season_colors = {
        "Winter": "#38BDF8",
        "Summer": "#F59E0B",
        "Summer or Pre-monsoon": "#F59E0B",
        "Monsoon": "#10B981",
        "Post-monsoon": "#8B5CF6",
    }

    available_seasons = [s for s in settings.SEASON_ORDER if s in df["season"].unique()]

    for season in available_seasons:
        s_df = df[(df["season"] == season) & (df["location"].isin(locs_to_show))]
        s_df = s_df.set_index("location").reindex(locs_to_show).reset_index()
        fig.add_trace(
            go.Bar(
                x=s_df["location"],
                y=s_df["share_of_annual_pct"],
                name=season,
                marker=dict(color=season_colors.get(season, "#94A3B8")),
                hovertemplate=f"<b>{season}</b><br>Station: %{{x}}<br>Share: %{{y:.1f}} %<extra></extra>",
            )
        )

    fig.update_layout(
        barmode="stack",
        xaxis=dict(title="Station"),
        yaxis=dict(title="Share of Annual Precipitation (%)", range=[0, 100]),
    )
    return apply_theme(fig, title="Seasonal Rainfall Partitioning (% of Annual Total)")


def build_rain_unusual_months(df: pd.DataFrame, location: str | None = None) -> tuple[go.Figure, pd.DataFrame]:
    """Z-score scatter over time for ONE station with threshold line at 2.0 and flagged months."""
    if df.empty:
        return build_empty_figure(), pd.DataFrame()

    locs = sorted(df["location"].unique()) if "location" in df.columns else []
    target_loc = location if (location and location in locs) else (locs[0] if locs else None)

    loc_df = df[df["location"] == target_loc].copy() if target_loc else df.copy()
    if loc_df.empty:
        return build_empty_figure(f"No rainfall records for station '{target_loc}'"), pd.DataFrame()

    loc_df["date_str"] = (
        loc_df["year"].astype(str) + "-" + loc_df["month"].astype(str).str.zfill(2)
    )
    loc_df = loc_df.sort_values(["year", "month"])

    fig = go.Figure()
    # Normal points
    fig.add_trace(
        go.Scatter(
            x=loc_df["date_str"],
            y=loc_df["z_score"],
            mode="lines+markers",
            name="Z-Score",
            line=dict(color="#64748B", width=1.5),
            marker=dict(size=4),
            hovertemplate="Month: %{x}<br>Z-Score: %{y:.2f}<extra></extra>",
        )
    )

    # Flagged anomaly points
    flagged = (
        loc_df[loc_df["is_unusually_high"] == True]
        if "is_unusually_high" in loc_df.columns
        else pd.DataFrame()
    )
    if not flagged.empty:
        fig.add_trace(
            go.Scatter(
                x=flagged["date_str"],
                y=flagged["z_score"],
                mode="markers",
                name=f"Anomaly (Z >= {settings.RAINFALL_ZSCORE_THRESHOLD})",
                marker=dict(color="#EF4444", size=9, symbol="diamond"),
                hovertemplate="<b>UNUSUAL RAINFALL</b><br>Month: %{x}<br>Z-Score: %{y:.2f}<extra></extra>",
            )
        )

    # Threshold horizontal reference line
    fig.add_hline(
        y=settings.RAINFALL_ZSCORE_THRESHOLD,
        line=dict(color="#F59E0B", dash="dash", width=1.5),
        annotation_text=f"Threshold (Z = {settings.RAINFALL_ZSCORE_THRESHOLD})",
        annotation_position="top right",
    )

    fig.update_layout(
        xaxis=dict(title="Year-Month", tickangle=-45, nticks=20),
        yaxis=dict(title="Standardized Anomaly (Z-Score)"),
    )
    title = f"Unusual Rainfall Anomalies (Z-Score) — {target_loc}" if target_loc else "Unusual Rainfall Anomalies"
    apply_theme(fig, title=title)
    return fig, flagged


def build_rain_heavy_days(df: pd.DataFrame, location: str | None = None) -> go.Figure:
    """Stacked bar chart per year categorized by IMD precipitation classes for ONE station."""
    if df.empty:
        return build_empty_figure()

    locs = sorted(df["location"].unique()) if "location" in df.columns else []
    target_loc = location if (location and location in locs) else (locs[0] if locs else None)

    loc_df = df[df["location"] == target_loc].sort_values("year") if target_loc else df.sort_values("year")
    if loc_df.empty:
        return build_empty_figure(f"No heavy rain events for station '{target_loc}'")

    fig = go.Figure()
    n_rows = len(loc_df)
    heavy_vals = loc_df["heavy_rain_days"] if "heavy_rain_days" in loc_df.columns else [0] * n_rows
    very_heavy_vals = loc_df["very_heavy_rain_days"] if "very_heavy_rain_days" in loc_df.columns else [0] * n_rows
    extreme_vals = loc_df["extremely_heavy_rain_days"] if "extremely_heavy_rain_days" in loc_df.columns else [0] * n_rows

    fig.add_trace(
        go.Bar(
            x=loc_df["year"],
            y=heavy_vals,
            name="Heavy (64.5 - 115.5 mm)",
            marker=dict(color="#38BDF8"),
            hovertemplate="Year: %{x}<br>Heavy: %{y} days<extra></extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            x=loc_df["year"],
            y=very_heavy_vals,
            name="Very Heavy (115.6 - 204.4 mm)",
            marker=dict(color="#F59E0B"),
            hovertemplate="Year: %{x}<br>Very Heavy: %{y} days<extra></extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            x=loc_df["year"],
            y=extreme_vals,
            name="Extremely Heavy (>= 204.5 mm)",
            marker=dict(color="#EF4444"),
            hovertemplate="Year: %{x}<br>Extremely Heavy: %{y} days<extra></extra>",
        )
    )

    fig.update_layout(
        barmode="stack",
        xaxis=dict(title="Year"),
        yaxis=dict(title="Number of Days"),
    )
    title = f"Annual Deluge Event Days by IMD Class — {target_loc}" if target_loc else "Annual Deluge Event Days"
    return apply_theme(fig, title=title)


# =============================================================================
# 3. CLIMATE PATTERN VISUALIZATION BUILDERS
# =============================================================================

def build_climate_year_comparison(df: pd.DataFrame, year_a: int = 2015, year_b: int = 2020) -> go.Figure:
    """Diverging horizontal bar chart comparing percentage shift between Year A and Year B."""
    if df.empty or "metric" not in df.columns or "pct_change" not in df.columns:
        return build_empty_figure()

    sorted_df = df.sort_values("pct_change")
    colors = ["#EF4444" if val > 0 else "#38BDF8" for val in sorted_df["pct_change"]]

    fig = go.Figure(
        go.Bar(
            y=sorted_df["metric"],
            x=sorted_df["pct_change"],
            orientation="h",
            marker=dict(color=colors),
            hovertemplate="<b>%{y}</b><br>Shift: %{x:+.2f} %<extra></extra>",
        )
    )
    fig.update_layout(
        xaxis=dict(title=f"Percentage Change: {year_b} relative to {year_a} (%)"),
        yaxis=dict(title="Metric"),
    )
    return apply_theme(fig, title=f"Inter-Annual Shift: {year_a} vs. {year_b}")


def build_climate_city_profile(df: pd.DataFrame, metric: str = "avg_temp") -> go.Figure:
    """Bar chart ranking stations on a user-chosen climate metric."""
    if df.empty or "location" not in df.columns:
        return build_empty_figure()

    if metric not in df.columns:
        metric = "avg_temp" if "avg_temp" in df.columns else df.columns[1]

    sorted_df = df.sort_values(metric, ascending=True)
    colors = [get_location_color(loc) for loc in sorted_df["location"]]

    unit = UNITS.get(metric, "")
    unit_str = f" ({unit})" if unit else ""

    fig = go.Figure(
        go.Bar(
            y=sorted_df["location"],
            x=sorted_df[metric],
            orientation="h",
            marker=dict(color=colors),
            hovertemplate=f"Station: %{{y}}<br>{metric}: %{{x:,.2f}}{unit_str}<extra></extra>",
        )
    )
    fig.update_layout(
        xaxis=dict(title=f"{metric}{unit_str}"),
        yaxis=dict(title="Station"),
    )
    return apply_theme(fig, title=f"Station Comparison: {metric}")


def build_extreme_events_yearly(df: pd.DataFrame, metric: str = "hot_days") -> go.Figure:
    """Annual trend lines per station for a selected extreme event category."""
    if df.empty or "location" not in df.columns:
        return build_empty_figure()

    if metric not in df.columns:
        metric = "hot_days" if "hot_days" in df.columns else df.columns[2]

    fig = go.Figure()
    all_locs = sorted(df["location"].unique())
    locs_to_show = _check_location_limit(fig, all_locs)

    for loc in locs_to_show:
        loc_df = df[df["location"] == loc].sort_values("year")
        fig.add_trace(
            go.Scatter(
                x=loc_df["year"],
                y=loc_df[metric],
                mode="lines+markers",
                name=loc,
                line=dict(color=get_location_color(loc), width=2),
                marker=dict(size=5),
                hovertemplate=f"<b>{loc}</b><br>Year: %{{x}}<br>{metric}: %{{y}} days<extra></extra>",
            )
        )

    fig.update_layout(
        xaxis=dict(title="Year"),
        yaxis=dict(title=f"Days Breaching Threshold ({metric})"),
    )
    return apply_theme(fig, title=f"Annual Extreme Event Frequency ({metric.replace('_', ' ').title()})")


def build_heatwave_spells(df: pd.DataFrame) -> go.Figure:
    """Bar chart of heatwave spells at spell_start with height length_days and peak_temp_max in hover."""
    if df.empty or "spell_start" not in df.columns or "length_days" not in df.columns:
        return build_empty_figure("No prolonged heatwave spells (>= 3 days) detected")

    sorted_df = df.sort_values("spell_start")

    hover_texts = []
    for _, r in sorted_df.iterrows():
        txt = (
            f"<b>{r.get('location', '')}</b><br>"
            f"Start: {r['spell_start']}<br>"
            f"End: {r.get('spell_end', '')}<br>"
            f"Duration: {r['length_days']} days<br>"
            f"Peak T-Max: {r.get('peak_temp_max', 'n/a')} °C"
        )
        hover_texts.append(txt)

    colors = [get_location_color(loc) for loc in sorted_df.get("location", [""] * len(sorted_df))]

    fig = go.Figure(
        go.Bar(
            x=sorted_df["spell_start"],
            y=sorted_df["length_days"],
            marker=dict(color=colors),
            hovertext=hover_texts,
            hoverinfo="text",
        )
    )
    fig.update_layout(
        xaxis=dict(title="Spell Start Date", tickangle=-45),
        yaxis=dict(title="Duration (Consecutive Days)"),
    )
    return apply_theme(fig, title="Prolonged Heatwave Spells (Gaps-and-Islands Discovery)")


def build_correlations(
    df: pd.DataFrame, mode: str = "raw", scope: str = "all"
) -> go.Figure:
    """
    Annotated 5x5 symmetric Pearson correlation heatmap.
    Zero-centered diverging colorscale [-1, 1].
    """
    if df.empty or "var_x" not in df.columns or "var_y" not in df.columns or "r" not in df.columns:
        return build_empty_figure()

    variables = settings.CORRELATION_VARIABLES
    matrix = pd.DataFrame(index=variables, columns=variables, dtype=float)

    # Fill diagonal with 1.0
    for v in variables:
        matrix.loc[v, v] = 1.0

    # Fill off-diagonals from Spark SQL results
    for _, row in df.iterrows():
        vx = row["var_x"]
        vy = row["var_y"]
        val = float(row["r"]) if pd.notnull(row["r"]) else 0.0
        if vx in variables and vy in variables:
            matrix.loc[vx, vy] = val
            matrix.loc[vy, vx] = val

    z_vals = matrix.to_numpy()

    # Create formatted annotations for each cell
    annotations = []
    for i, row_var in enumerate(variables):
        for j, col_var in enumerate(variables):
            val = z_vals[i, j]
            val_str = f"{val:+.2f}" if not math.isnan(val) else "—"
            annotations.append(
                dict(
                    x=col_var,
                    y=row_var,
                    text=val_str,
                    showarrow=False,
                    font=dict(color="#FFFFFF" if abs(val) > 0.4 else "#E2E8F0", size=13),
                )
            )

    min_pairs = int(df["n_pairs"].min()) if "n_pairs" in df.columns and not df.empty else 0

    fig = go.Figure(
        go.Heatmap(
            z=z_vals,
            x=variables,
            y=variables,
            colorscale=settings.DIVERGING_COLORSCALE,
            zmin=-1.0,
            zmax=1.0,
            zmid=0.0,
            colorbar=dict(title="Pearson r", len=0.8),
            hovertemplate="Pair: %{y} vs %{x}<br>r: %{z:+.3f}<extra></extra>",
        )
    )
    fig.update_layout(
        annotations=annotations,
        xaxis=dict(title="Atmospheric Variable"),
        yaxis=dict(title="Atmospheric Variable", autorange="reversed"),
    )

    mode_label = "Raw Observations" if mode == "raw" else "Deseasonalized Anomalies"
    scope_label = "All Stations" if scope == "all" else f"Station: {scope}"
    title = f"Correlation Matrix ({mode_label} · {scope_label} · N_pairs >= {min_pairs:,})"

    return apply_theme(fig, title=title)

"""
src/dashboard/theme.py
======================
Plotly "weather_warm" theme registration and layout utilities.
Implements the warm light design reference palette, typography, and axes styling.
"""

from __future__ import annotations

import plotly.graph_objects as go
import plotly.io as pio

from config.settings import (
    COLORS,
    LOCATION_PALETTE,
    PLOTLY_TEMPLATE_NAME,
)

# Build the layout template
_warm_layout = go.Layout(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(
        family="Plus Jakarta Sans, sans-serif",
        color="#1F1815",
        size=12,
    ),
    colorway=LOCATION_PALETTE,
    xaxis=dict(
        gridcolor="#E5DDD0",
        gridwidth=1,
        linecolor="#D3C5AD",
        linewidth=1,
        zerolinecolor="#E5DDD0",
        tickfont=dict(
            family="Plus Jakarta Sans, sans-serif",
            color="#7A6E65",
            size=11,
        ),
        title=dict(
            font=dict(
                family="Plus Jakarta Sans, sans-serif",
                color="#1F1815",
                size=12,
            )
        ),
    ),
    yaxis=dict(
        gridcolor="#E5DDD0",
        gridwidth=1,
        linecolor="#D3C5AD",
        linewidth=1,
        zerolinecolor="#E5DDD0",
        tickfont=dict(
            family="Plus Jakarta Sans, sans-serif",
            color="#7A6E65",
            size=11,
        ),
        title=dict(
            font=dict(
                family="Plus Jakarta Sans, sans-serif",
                color="#1F1815",
                size=12,
            )
        ),
    ),
    hoverlabel=dict(
        bgcolor="#FFFFFF",
        bordercolor="#E5DDD0",
        font=dict(
            family="Plus Jakarta Sans, sans-serif",
            color="#1F1815",
            size=12,
        ),
    ),
    legend=dict(
        orientation="v",
        yanchor="top",
        y=1.0,
        xanchor="left",
        x=1.02,
        bgcolor="rgba(255, 255, 255, 0.85)",
        bordercolor="#E5DDD0",
        borderwidth=1,
        font=dict(
            family="Plus Jakarta Sans, sans-serif",
            color="#1F1815",
            size=11,
        ),
    ),
    margin=dict(l=50, r=40, t=50, b=50),
)

# Register template globally in Plotly's registry
pio.templates[PLOTLY_TEMPLATE_NAME] = go.layout.Template(layout=_warm_layout)
pio.templates.default = PLOTLY_TEMPLATE_NAME


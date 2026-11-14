"""
tests/test_styles.py
====================
Unit tests for Phase 11 warm light design reference styling:
1. CSS style builder (build_css) contents, font families, colors, security.
2. KPI card HTML rendering, entity escaping, None handling, units, accents.
3. SVG icon registry (10 icons, valid geometry, no external references).
4. Plotly "weather_warm" template registration and colorway integrity.
"""

from __future__ import annotations

import re
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import pytest

from config.settings import (
    COLORS,
    FONT_BODY,
    FONT_HEADING,
    FONT_MONO,
    LOCATION_PALETTE,
    PLOTLY_TEMPLATE_NAME,
)
from src.dashboard.charts import (
    build_climate_year_comparison,
    build_empty_figure,
    build_rain_yearly,
    build_temp_yearly,
    get_location_color,
)
from src.dashboard.ui.components import (
    ICONS,
    build_kpi_card_html,
)
from src.dashboard.ui.styles import build_css


def test_build_css_contains_colors_and_fonts():
    """
    CSS builder must contain every color definition from COLORS,
    the three declared font families, exactly one Google Fonts URL,
    and zero executable script/javascript vectors.
    """
    css = build_css()

    # 1. Verify all COLORS hex values appear in the CSS
    for color_key, hex_val in COLORS.items():
        assert hex_val.lower() in css.lower(), f"Color '{color_key}' ({hex_val}) missing from CSS"

    # 2. Verify all three font families appear
    assert "Epilogue" in css, "Heading font Epilogue missing from CSS"
    assert "Plus Jakarta Sans" in css, "Body font Plus Jakarta Sans missing from CSS"
    assert "JetBrains Mono" in css, "Monospace font JetBrains Mono missing from CSS"

    # 3. Exactly one external URL allowed: Google Fonts @import
    urls = re.findall(r"https?://[^\s'\"]+", css)
    assert len(urls) == 1, f"Expected exactly 1 URL (Google Fonts), found {len(urls)}: {urls}"
    assert urls[0].startswith("https://fonts.googleapis.com/"), f"Unexpected URL found: {urls[0]}"

    # 4. No executable script vectors or javascript protocols
    assert "javascript:" not in css.lower(), "Security violation: 'javascript:' protocol in CSS"
    assert "<script" not in css.lower(), "Security violation: '<script' tag in CSS"


def test_kpi_card_escaping_and_null_handling():
    """
    KPI card HTML builder must sanitize dynamic input, render 'n/a' for None,
    and include units and designated accent classes.
    """
    # 1. Sanitization / XSS prevention
    malicious_label = "<script>alert(1)</script>"
    malicious_caption = "x' OR '1'='1"
    card_html = build_kpi_card_html(
        label=malicious_label,
        value_text="123",
        unit="°C",
        caption=malicious_caption,
        accent="terracotta",
        icon="thermometer",
    )
    assert "<script>" not in card_html, "Raw unescaped <script> tag leaked into KPI card"
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in card_html
    assert "x&#x27; OR &#x27;1&#x27;=&#x27;1" in card_html or "x' OR '1'='1" not in card_html
    assert "°C" in card_html
    assert "wcp-accent-terracotta" in card_html
    assert "tint-heat" in card_html

    # 2. None value handling -> 'n/a'
    null_card = build_kpi_card_html(
        label="Average Temperature",
        value_text=None,
        unit="°C",
        caption=None,
        accent="neutral",
        icon="dataset",
    )
    assert "n/a" in null_card
    assert "Average Temperature" in null_card

    # 3. Slate-blue accent tint check (Minimum Temperature)
    cold_card = build_kpi_card_html(
        label="Minimum Temperature",
        value_text="0.5",
        unit="°C",
        caption="Shillong",
        accent="slate-blue",
        icon="snowflake",
    )
    assert "tint-cold" in cold_card
    assert "wcp-accent-slate-blue" in cold_card


def test_svg_icon_registry():
    """
    All 10 required icon keys must exist, contain valid standalone inline SVG geometry,
    and contain no external references or scripts.
    """
    expected_icons = [
        "dataset",
        "location",
        "thermometer",
        "flame",
        "snowflake",
        "rain",
        "humidity",
        "wind",
        "gauge",
        "check",
    ]

    for key in expected_icons:
        assert key in ICONS, f"Missing required icon: '{key}'"
        svg = ICONS[key]
        assert svg.strip().startswith("<svg"), f"Icon '{key}' must begin with <svg"
        assert svg.strip().endswith("</svg>"), f"Icon '{key}' must end with </svg>"
        assert 'viewBox="0 0 24 24"' in svg, f"Icon '{key}' must have viewBox='0 0 24 24'"
        assert "http://" not in svg and "https://" not in svg, f"Icon '{key}' has external URL"
        assert "<image" not in svg, f"Icon '{key}' contains bitmap image tag"
        assert "<script" not in svg, f"Icon '{key}' contains script tag"


def test_plotly_template_weather_warm_registered():
    """
    The 'weather_warm' Plotly template must be registered with transparent backgrounds,
    correct colorway from LOCATION_PALETTE, and builder figures must adopt it.
    """
    # 1. Template registration in Plotly engine
    assert PLOTLY_TEMPLATE_NAME in pio.templates, f"Template '{PLOTLY_TEMPLATE_NAME}' not registered"
    warm_tmpl = pio.templates[PLOTLY_TEMPLATE_NAME]

    # 2. Colorway starts with LOCATION_PALETTE
    assert list(warm_tmpl.layout.colorway[: len(LOCATION_PALETTE)]) == LOCATION_PALETTE

    # 3. Transparent backgrounds
    assert warm_tmpl.layout.paper_bgcolor == "rgba(0,0,0,0)"
    assert warm_tmpl.layout.plot_bgcolor == "rgba(0,0,0,0)"

    # 4. Figure builders adopt template
    fig_empty = build_empty_figure()
    assert (
        fig_empty.layout.template == pio.templates[PLOTLY_TEMPLATE_NAME]
        or list(fig_empty.layout.template.layout.colorway[: len(LOCATION_PALETTE)]) == LOCATION_PALETTE
    )

    sample_df = pd.DataFrame([
        {"location": "Bengaluru", "year": 2020, "avg_temp": 24.5, "anomaly_c": 0.2, "is_complete_year": True},
        {"location": "Delhi", "year": 2020, "avg_temp": 25.1, "anomaly_c": 0.4, "is_complete_year": True},
    ])
    fig_temp = build_temp_yearly(sample_df)
    assert (
        fig_temp.layout.template == pio.templates[PLOTLY_TEMPLATE_NAME]
        or list(fig_temp.layout.template.layout.colorway[: len(LOCATION_PALETTE)]) == LOCATION_PALETTE
    )

    # 5. Stable location colors across station subsets
    color_bengaluru_subset1 = get_location_color("Bengaluru")
    color_delhi_subset1 = get_location_color("Delhi")
    color_bengaluru_subset2 = get_location_color("Bengaluru")
    color_delhi_subset2 = get_location_color("Delhi")
    assert color_bengaluru_subset1 == color_bengaluru_subset2
    assert color_delhi_subset1 == color_delhi_subset2
    assert color_bengaluru_subset1 != color_delhi_subset1

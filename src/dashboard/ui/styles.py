"""
src/dashboard/ui/styles.py
==========================
Warm light design reference CSS styles and dynamic style injector for Streamlit.
Prefix for custom styling classes: .wcp- (Weather Climate Pattern).

Internal Streamlit Selectors Reference (for future upgrade checks):
- header[data-testid="stHeader"]: top Streamlit app header
- [data-testid="stAppDeployButton"]: deploy button in header (hidden)
- footer: Streamlit default footer (hidden)
- section[data-testid="stSidebar"]: left sidebar container
- [data-testid="stSidebarCollapseButton"]: sidebar collapse toggle (preserved visible)
- [data-testid="stMainMenu"]: top-right main menu (preserved visible)
- div[data-testid="stMetric"]: fallback metric container
- div[data-testid="stRadio"]: radio button group container
- div[data-testid="stExpander"]: expander container
- div[data-testid="stFileUploader"]: file uploader container
- [data-baseweb="tag"]: multiselect pill tags
- div[data-testid="stFormSubmitButton"] button: form submit buttons
"""

from __future__ import annotations

import streamlit as st
from config.settings import (
    COLORS,
    FONT_BODY,
    FONT_HEADING,
    FONT_MONO,
)


def build_css() -> str:
    """
    Build the consolidated CSS string containing all design reference rules,
    colors, typography, and component styling.
    """
    c = COLORS

    css = f"""
/* Google Fonts Import: Headings (Epilogue), Body (Plus Jakarta Sans), Numbers/Code (JetBrains Mono) */
@import url('https://fonts.googleapis.com/css2?family=Epilogue:wght@500;600;700;800&family=JetBrains+Mono:wght@400;500;600&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');

:root {{
    --wcp-page-bg: {c['page_bg']};
    --wcp-surface: {c['surface']};
    --wcp-card-bg: {c['card_bg']};
    --wcp-card-border: {c['card_border']};
    --wcp-card-border-hover: {c['card_border_hover']};
    --wcp-text-primary: {c['text_primary']};
    --wcp-text-muted: {c['text_muted']};
    --wcp-label-muted: {c['label_muted']};
    --wcp-accent-mustard: {c['accent_mustard']};
    --wcp-accent-text: {c['accent_text']};
    --wcp-heat-terracotta: {c['heat_terracotta']};
    --wcp-rain-green: {c['rain_green']};
    --wcp-cold-slate: {c['cold_slate']};
    --wcp-wind-slate: {c['wind_slate']};
    --wcp-success-green: {c['success_green']};
    --wcp-caution-bg: {c['caution_bg']};
    --wcp-caution-border: {c['caution_border']};
    --wcp-error-text: {c['error_text']};
    --wcp-error-bg: {c['error_bg']};
    --wcp-active-pill-bg: {c['active_pill_bg']};
    --wcp-active-pill-text: {c['active_pill_text']};
    --wcp-code-bg: {c['code_bg']};
    --wcp-code-border: {c['code_border']};
    --wcp-code-text: {c['code_text']};
    --wcp-code-keyword: {c['code_keyword']};
    --wcp-code-number: {c['code_number']};
}}

/* Global Typography & Background */
html, body, [data-testid="stAppViewContainer"] {{
    background-color: {c['page_bg']};
    color: {c['text_primary']};
    font-family: {FONT_BODY};
}}

h1, h2, h3, h4, h5, h6 {{
    font-family: {FONT_HEADING};
    color: {c['text_primary']};
    font-weight: 700;
}}

/* Header & Footer Streamlit internals: hide deploy button and footer, preserve collapse and menu */
header[data-testid="stHeader"] {{
    background-color: transparent !important;
}}
[data-testid="stAppDeployButton"] {{
    display: none !important;
}}
footer {{
    display: none !important;
}}

/* Sidebar Styling */
section[data-testid="stSidebar"] {{
    background-color: {c['surface']};
    border-right: 1px solid {c['card_border']};
    width: 280px !important;
    min-width: 280px !important;
}}
section[data-testid="stSidebar"] .block-container {{
    padding-top: 1.5rem;
    padding-left: 1rem;
    padding-right: 1rem;
}}

/* Sidebar Card Containers */
.wcp-sidebar-card {{
    background-color: {c['card_bg']};
    border: 1px solid {c['card_border']};
    border-radius: 12px;
    padding: 14px 16px;
    margin-bottom: 14px;
    box-shadow: 0 2px 8px -2px rgba(61,49,42,0.05);
}}
.wcp-sidebar-card-title {{
    font-family: {FONT_HEADING};
    font-size: 0.75rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: {c['label_muted']};
    margin-bottom: 8px;
}}

/* General Buttons */
.stButton > button {{
    font-family: {FONT_BODY};
    font-weight: 600;
    border-radius: 8px;
    padding: 6px 14px;
    transition: all 0.15s ease-in-out;
}}
.stButton > button[kind="primary"] {{
    background-color: {c['accent_mustard']} !important;
    color: {c['accent_text']} !important;
    border: 1px solid {c['accent_mustard']} !important;
    box-shadow: 0 2px 8px rgba(246, 186, 34, 0.35);
}}
.stButton > button[kind="primary"]:hover {{
    background-color: #E8AF1F !important;
}}
.stButton > button[kind="secondary"], .stButton > button:not([kind="primary"]) {{
    background-color: {c['surface']} !important;
    color: {c['text_primary']} !important;
    border: 1px solid {c['card_border']} !important;
}}
.stButton > button:not([kind="primary"]):hover {{
    border-color: {c['card_border_hover']} !important;
    background-color: #F2ECE1 !important;
}}

/* Apply Filters Button (Form Submit) */
div[data-testid="stFormSubmitButton"] > button {{
    background-color: {c['active_pill_bg']} !important;
    color: {c['active_pill_text']} !important;
    border: 1px solid {c['active_pill_bg']} !important;
    width: 100%;
}}
div[data-testid="stFormSubmitButton"] > button:hover {{
    background-color: #332722 !important;
    border-color: #332722 !important;
}}

/* Multiselect, Selectbox, Inputs */
div[data-baseweb="select"] {{
    border-radius: 8px;
}}
div[data-baseweb="tag"] {{
    background-color: {c['active_pill_bg']} !important;
    color: {c['active_pill_text']} !important;
    border-radius: 16px;
    font-family: {FONT_BODY};
    font-size: 0.8rem;
    font-weight: 500;
}}
div[data-baseweb="tag"] span {{
    color: {c['active_pill_text']} !important;
}}

/* Checkbox accent */
input[type="checkbox"]:checked {{
    accent-color: {c['heat_terracotta']} !important;
}}

/* File Uploader Dropzone */
div[data-testid="stFileUploader"] section {{
    background-color: #FBF9F4 !important;
    border: 2px dashed {c['heat_terracotta']} !important;
    border-radius: 12px;
}}

/* Horizontal Section Radio Navigation Styled as Pill Tabs */
div[data-testid="stRadio"][aria-label="Select View"] > div[role="radiogroup"],
div[data-testid="stRadio"] > div[role="radiogroup"] {{
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    background-color: transparent;
    border: none;
    padding: 4px 0 16px 0;
}}
/* Hide default radio circle inputs */
div[data-testid="stRadio"] div[role="radiogroup"] label > div:first-child {{
    display: none !important;
}}
/* Style the label as a pill button */
div[data-testid="stRadio"] div[role="radiogroup"] label {{
    background-color: {c['surface']};
    border: 1px solid {c['card_border']};
    border-radius: 20px;
    padding: 6px 16px;
    cursor: pointer;
    transition: all 0.15s ease-in-out;
    margin: 0;
}}
div[data-testid="stRadio"] div[role="radiogroup"] label:hover {{
    background-color: #F2ECE1;
    border-color: {c['card_border_hover']};
}}
div[data-testid="stRadio"] div[role="radiogroup"] label p {{
    font-family: {FONT_BODY};
    font-size: 0.88rem;
    font-weight: 500;
    color: {c['text_muted']};
    margin: 0 !important;
}}
/* Active radio option (matches aria-checked=true on the label/input) */
div[data-testid="stRadio"] div[role="radiogroup"] label:has(input:checked) {{
    background-color: {c['active_pill_bg']} !important;
    border-color: {c['active_pill_bg']} !important;
}}
div[data-testid="stRadio"] div[role="radiogroup"] label:has(input:checked) p {{
    color: {c['active_pill_text']} !important;
    font-weight: 700 !important;
}}

/* Expanders */
div[data-testid="stExpander"] {{
    background-color: {c['card_bg']} !important;
    border: 1px solid {c['card_border']} !important;
    border-radius: 12px !important;
    margin-bottom: 12px;
    box-shadow: 0 2px 8px -2px rgba(61,49,42,0.05);
}}
div[data-testid="stExpander"] summary {{
    font-family: {FONT_HEADING};
    font-weight: 600;
    color: {c['text_primary']};
}}

/* Code Blocks: Espresso Background */
div[data-testid="stCode"], pre, code {{
    font-family: {FONT_MONO} !important;
}}
div[data-testid="stCode"] {{
    background-color: {c['code_bg']} !important;
    border: 1px solid {c['code_border']} !important;
    border-radius: 8px !important;
}}
div[data-testid="stCode"] pre {{
    background-color: {c['code_bg']} !important;
    color: {c['code_text']} !important;
}}

/* DataFrames and Tables */
div[data-testid="stDataFrame"], table {{
    border: 1px solid {c['card_border']};
    border-radius: 8px;
    overflow: hidden;
}}

/* Fallback Metric Container */
div[data-testid="stMetric"] {{
    background-color: {c['card_bg']} !important;
    border: 1px solid {c['card_border']} !important;
    border-radius: 12px !important;
    padding: 14px 18px !important;
    box-shadow: 0 2px 8px -2px rgba(61,49,42,0.05);
}}
div[data-testid="stMetricLabel"] p {{
    font-family: {FONT_BODY};
    font-size: 0.75rem !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: {c['label_muted']} !important;
}}
div[data-testid="stMetricValue"] div {{
    font-family: {FONT_HEADING};
    font-size: 1.5rem !important;
    font-weight: 700;
    color: {c['text_primary']} !important;
}}

/* Custom WCP Component Classes */
.wcp-card {{
    background-color: {c['card_bg']};
    border: 1px solid {c['card_border']};
    border-radius: 12px;
    padding: 18px 20px;
    margin-bottom: 16px;
    box-shadow: 0 2px 8px -2px rgba(61,49,42,0.05);
}}
.wcp-card:hover {{
    border-color: {c['card_border_hover']};
}}

.wcp-header-block {{
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin-bottom: 12px;
}}
.wcp-header-title-row {{
    display: flex;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
}}
.wcp-header-title {{
    font-family: {FONT_HEADING};
    font-size: 1.75rem;
    font-weight: 800;
    color: {c['text_primary']};
    margin: 0;
}}
.wcp-badge {{
    display: inline-flex;
    align-items: center;
    background-color: {c['accent_mustard']};
    color: {c['accent_text']};
    font-family: {FONT_BODY};
    font-size: 0.75rem;
    font-weight: 700;
    border-radius: 12px;
    padding: 3px 10px;
    letter-spacing: 0.02em;
}}
.wcp-header-subtitle {{
    font-family: {FONT_BODY};
    font-size: 0.9rem;
    color: {c['text_muted']};
    margin: 0;
}}

/* Dataset Status Strip */
.wcp-status-strip {{
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 16px;
    background-color: {c['card_bg']};
    border: 1px solid {c['card_border']};
    border-radius: 10px;
    padding: 10px 16px;
    margin-bottom: 14px;
    font-size: 0.85rem;
    color: {c['text_muted']};
    box-shadow: 0 1px 4px rgba(61,49,42,0.03);
}}
.wcp-status-item {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
}}
.wcp-status-dot {{
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background-color: {c['success_green']};
    display: inline-block;
}}
.wcp-status-val {{
    font-family: {FONT_MONO};
    font-weight: 600;
    color: {c['text_primary']};
}}
.wcp-status-interp-amber {{
    color: {c['caution_border']} !important;
}}

/* Custom KPI Card */
.wcp-kpi-card {{
    background-color: {c['card_bg']};
    border: 1px solid {c['card_border']};
    border-radius: 12px;
    padding: 14px 16px;
    min-height: 128px;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    box-shadow: 0 2px 8px -2px rgba(61,49,42,0.05);
    transition: border-color 0.15s ease-in-out;
}}
.wcp-kpi-card:hover {{
    border-color: {c['card_border_hover']};
}}
.wcp-kpi-card.tint-heat {{
    background-color: #FFF8F5 !important;
}}
.wcp-kpi-card.tint-cold {{
    background-color: #F5F8FC !important;
}}
.wcp-kpi-top {{
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 8px;
}}
.wcp-kpi-label {{
    font-family: {FONT_BODY};
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: {c['label_muted']};
    line-height: 1.2;
}}
.wcp-kpi-icon-tile {{
    width: 32px;
    height: 32px;
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
}}
.wcp-kpi-val-row {{
    display: flex;
    align-items: baseline;
    gap: 4px;
    margin: 4px 0 2px 0;
}}
.wcp-kpi-val {{
    font-family: {FONT_HEADING};
    font-size: 1.55rem;
    font-weight: 800;
    color: {c['text_primary']};
    line-height: 1.1;
}}
.wcp-kpi-unit {{
    font-family: {FONT_BODY};
    font-size: 0.85rem;
    font-weight: 600;
}}
.wcp-kpi-caption {{
    font-family: {FONT_BODY};
    font-size: 0.75rem;
    color: {c['text_muted']};
    line-height: 1.25;
    display: flex;
    align-items: center;
    gap: 5px;
}}
.wcp-caption-dot {{
    width: 5px;
    height: 5px;
    border-radius: 50%;
    display: inline-block;
    flex-shrink: 0;
}}

/* Key Findings Row */
.wcp-finding-row {{
    background-color: {c['card_bg']};
    border: 1px solid {c['card_border']};
    border-radius: 10px;
    padding: 12px 16px;
    margin-bottom: 8px;
    box-shadow: 0 1px 4px rgba(61,49,42,0.03);
}}
.wcp-finding-row.wcp-caution {{
    background-color: {c['caution_bg']} !important;
    border-color: {c['caution_border']} !important;
}}
.wcp-category-tag {{
    display: inline-block;
    font-size: 0.7rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    padding: 2px 8px;
    border-radius: 6px;
    margin-right: 8px;
}}
.wcp-tag-temperature {{
    background-color: #FDE8E1;
    color: {c['heat_terracotta']};
}}
.wcp-tag-rainfall {{
    background-color: #E2F0EB;
    color: {c['rain_green']};
}}
.wcp-tag-extremes {{
    background-color: #FEF3E2;
    color: #D97706;
}}
.wcp-tag-relationships {{
    background-color: #EAEEF5;
    color: {c['wind_slate']};
}}
.wcp-tag-data_quality {{
    background-color: #FEF9EE;
    color: #B45309;
}}

/* Validation Error Box */
.wcp-error-card {{
    background-color: {c['error_bg']};
    border: 1px solid {c['error_text']};
    border-radius: 10px;
    padding: 12px 16px;
    color: {c['error_text']};
    margin-bottom: 12px;
}}
"""
    return css


def inject_styles():
    """Inject consolidated design reference CSS into Streamlit page."""
    css = build_css()
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


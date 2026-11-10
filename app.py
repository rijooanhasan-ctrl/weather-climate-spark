"""
app.py
======
Streamlit dashboard entry point – Phase 1 placeholder.

This file will grow in later phases to include data loading,
Spark SQL analysis, and Plotly visualisations.  For now it simply
confirms that the environment is ready and the UI framework is wired up.

Run with:
    streamlit run app.py
"""

import streamlit as st

# ── Page configuration ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Weather & Climate Pattern Discovery",
    page_icon="🌦️",
    layout="centered",
)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🌦️ Weather and Climate Pattern Discovery")
st.subheader("Powered by Apache Spark SQL & PySpark")

st.divider()

# ── Phase 1 status banner ─────────────────────────────────────────────────────
st.success("✅ Environment ready")

st.markdown(
    """
    **Phase 1 – Environment Setup** is complete.

    | Component | Status |
    |-----------|--------|
    | Python virtual environment | ✅ Created |
    | PySpark 3.5 | ✅ Installed |
    | Spark SQL engine | ✅ Verified |
    | Streamlit dashboard | ✅ Running |
    | Project skeleton | ✅ In place |
    | Git repository | ✅ Initialised |

    ---
    **Coming in later phases:**
    - 📥 Phase 2 – Data ingestion & schema definition  
    - 🔍 Phase 3 – Spark SQL analysis & pattern discovery  
    - 📊 Phase 4 – Interactive Plotly dashboard  
    """
)

st.divider()
st.caption("Weather & Climate Pattern Discovery · Phase 1 · Built with Streamlit + Apache Spark")


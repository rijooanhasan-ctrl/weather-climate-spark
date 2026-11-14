"""
scripts/verify_app_ui.py
========================
AppTest-based verification script for UI conformance check (Audit A6).
Runs against the built-in dataset and checks all required UI components.
"""

from __future__ import annotations

import sys
from pathlib import Path
from streamlit.testing.v1 import AppTest

# Ensure repository root is on sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.utils import display_path
from src.dashboard.catalog import SECTIONS
from src.dashboard.services import ActiveDatasetStore


def main() -> int:
    print("=" * 70)
    print("A6 APPTEST UI CONFORMANCE CHECK")
    print("=" * 70)

    # Reset store to guarantee clean state
    store = ActiveDatasetStore.get_instance()
    store.reset()

    app_path = str(REPO_ROOT / "app.py")
    print(f"Loading AppTest from: {display_path(app_path)}")

    at = AppTest.from_file(app_path, default_timeout=240).run()

    if at.exception:
        print(f"FAILED: AppTest raised exceptions: {at.exception}")
        return 1

    print("AppTest ran cleanly with 0 exceptions.\n")

    # 1. Verify header strip text
    print("--- 1. Header Strip Verification ---")
    titles = [t.value for t in at.title]
    print(f"Titles found: {titles}")
    title_matches = any("Weather & Climate Pattern Discovery (Spark SQL)" in t for t in titles)
    print(f"Header contains 'Weather & Climate Pattern Discovery (Spark SQL)': {title_matches}")
    assert title_matches, "Title does not contain 'Weather & Climate Pattern Discovery (Spark SQL)'"

    markdown_texts = [m.value for m in at.markdown]
    dataset_matches = any("Built-in dataset" in m for m in markdown_texts)
    print(f"Header contains 'Built-in dataset': {dataset_matches}")
    assert dataset_matches, "Header strip does not contain 'Built-in dataset'"
    print("Header strip check: PASSED\n")

    # 2. Verify all sections in SECTIONS appear in UI
    print("--- 2. Section Navigation Verification ---")
    section_radios = [r for r in at.radio if r.key == "section"]
    assert len(section_radios) == 1, f"Expected 1 'section' radio widget, found {len(section_radios)}"
    rendered_options = section_radios[0].options
    print(f"Configured SECTIONS ({len(SECTIONS)}): {SECTIONS}")
    print(f"Rendered radio options ({len(rendered_options)}): {rendered_options}")
    assert set(rendered_options) == set(SECTIONS), "Rendered sections do not match SECTIONS catalog"
    print("Section navigation check: PASSED\n")

    # 3. Verify exactly 10 st.metric cards appear
    print("--- 3. Metric Cards Verification ---")
    metrics = at.metric
    print(f"Total st.metric widgets found: {len(metrics)}")
    for i, m in enumerate(metrics, start=1):
        print(f"  [{i:02d}/10] Label: '{m.label}' | Value: '{m.value}'")
    assert len(metrics) == 10, f"Expected exactly 10 metric cards, found {len(metrics)}"
    print("Metric cards check: PASSED\n")

    # 4. Verify sidebar filters
    print("--- 4. Sidebar Controls Verification ---")
    sb_multiselects = {ms.key: ms for ms in at.sidebar.multiselect}
    sb_dates = {d.key: d for d in at.sidebar.date_input}
    sb_buttons = {b.key: b for b in at.sidebar.button}

    print(f"Sidebar multiselect keys: {list(sb_multiselects.keys())}")
    print(f"Sidebar date keys: {list(sb_dates.keys())}")
    print(f"Sidebar button keys: {list(sb_buttons.keys())}")

    assert "sel_locations" in sb_multiselects, "Missing 'sel_locations' in sidebar"
    assert "sel_dates" in sb_dates, "Missing 'sel_dates' in sidebar"
    assert "sel_years" in sb_multiselects, "Missing 'sel_years' in sidebar"
    assert "sel_seasons" in sb_multiselects, "Missing 'sel_seasons' in sidebar"
    assert "btn_reset_filters" in sb_buttons, "Missing 'btn_reset_filters' in sidebar"
    print("Sidebar controls check: PASSED\n")

    print("=" * 70)
    print("ALL A6 CONFORMANCE CHECKS PASSED")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())

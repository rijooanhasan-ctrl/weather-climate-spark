"""
tests/test_app_smoke.py
=======================
Headless UI smoke tests for the Streamlit dashboard using AppTest.

LIMITATION NOTE:
----------------
Streamlit 1.40.2's `AppTest` does not currently support programmatic file
injection or interactive event simulation for `st.file_uploader`.
Consequently, the full upload and custom column-mapping workflow is exhaustively
verified at the service layer in `tests/test_dashboard_services.py` (including
tests 8, 9, 10, and 11) and via interactive manual browser testing.
"""

from pathlib import Path
import pytest
from streamlit.testing.v1 import AppTest

from src.dashboard.services import ActiveDatasetStore


@pytest.fixture(autouse=True)
def manage_store_lifecycle():
    """Reset the active dataset store before and after smoke testing."""
    store = ActiveDatasetStore.get_instance()
    store.reset()
    yield
    store.reset()


def test_app_smoke_built_in_flow():
    """
    Smoke test the full Streamlit UI lifecycle with the built-in dataset:
    - App starts cleanly with no unhandled exception elements.
    - Application title is rendered.
    - Overview displays headline KPI cards with formatted 'Total Records' = '46,752'.
    - Overview displays Key Findings with >= 5 insights.
    - Filtering by only 'Mumbai' updates 'Total Records' and insights.
    - Selecting no location triggers warning 'Select at least one location.' and hides metrics.
    """
    app_path = str(Path(__file__).resolve().parent.parent / "app.py")
    at = AppTest.from_file(app_path, default_timeout=240).run()

    # 1. Assert no unhandled exceptions occurred in the Streamlit app
    assert not at.exception, f"AppTest encountered unhandled exception(s): {at.exception}"

    # 2. Assert application title is present
    titles = [t.value for t in at.title] + [m.value for m in at.markdown]
    assert any("Weather and Climate Pattern Discovery" in t or "Weather & Climate Pattern Discovery" in t for t in titles)

    # 3. Assert headline KPI cards render all 10 values including 'Total Records' = 46,752
    # In Phase 11, KPI cards are rendered as custom warm-themed HTML cards via st.markdown
    total_rec_cards = [m.value for m in at.markdown if "Total Records" in m.value]
    assert len(total_rec_cards) > 0, "Missing 'Total Records' KPI card"
    assert "46,752" in total_rec_cards[0]

    # Verify all 10 KPI values are rendered in the dashboard cards
    expected_values = [
        "46,752",    # Total Records
        "8",         # Locations
        "24.6",      # Average Temperature
        "46.6",      # Maximum Temperature
        "0.5",       # Minimum Temperature
        "174,900.5", # Total Rainfall
        "68.9",      # Average Humidity
        "10.0",      # Average Wind Speed
        "1009.3",    # Average Pressure
        "0",         # Interpolated Values
    ]
    markdown_content = " ".join(m.value for m in at.markdown)
    for exp_val in expected_values:
        assert exp_val in markdown_content, f"Expected KPI value '{exp_val}' not found in cards"

    # 4. Assert Key Findings card renders >= 5 insights (info/warning boxes or finding rows)
    btn_compute = [b for b in at.button if "btn_compute_insights" in getattr(b, "key", "")]
    if btn_compute:
        btn_compute[0].click().run()
    info_and_warnings = [i.value for i in at.info] + [w.value for w in at.warning] + [m.value for m in at.markdown if "wcp-finding-row" in m.value]
    assert len(info_and_warnings) >= 5, f"Expected at least 5 insight callouts, got {len(info_and_warnings)}"

    # 5. Filter by only Mumbai (submitted via sidebar filter form)
    loc_multiselect = at.sidebar.multiselect(key="sel_locations")
    loc_multiselect.set_value(["Mumbai"])
    apply_btns = [b for b in at.sidebar.button if b.label == "Apply filters"]
    assert len(apply_btns) > 0, "Apply filters button not found in sidebar"
    apply_btns[0].click().run()

    assert not at.exception
    mumbai_cards = [m.value for m in at.markdown if "Total Records" in m.value]
    assert len(mumbai_cards) > 0
    # Mumbai has 5,844 daily records over 16 years (2010-2025)
    assert "5,844" in mumbai_cards[0]

    # Verify Mumbai insights are updated
    btn_compute_mumbai = [b for b in at.button if "btn_compute_insights" in getattr(b, "key", "")]
    if btn_compute_mumbai:
        btn_compute_mumbai[0].click().run()
    mumbai_infos = [i.value for i in at.info] + [m.value for m in at.markdown if "wcp-finding-row" in m.value]
    assert any("Mumbai" in inf for inf in mumbai_infos)

    # 6. Deselect all locations (submitted via sidebar filter form)
    loc_multiselect = at.sidebar.multiselect(key="sel_locations")
    loc_multiselect.set_value([])
    apply_btns = [b for b in at.sidebar.button if b.label == "Apply filters"]
    apply_btns[0].click().run()

    assert not at.exception
    warning_texts = [w.value for w in at.warning]
    assert any("Select at least one location." in w for w in warning_texts)

    # Assert KPI metrics are hidden when no location is selected
    empty_kpi_cards = [m.value for m in at.markdown if "Total Records" in m.value]
    assert len(empty_kpi_cards) == 0, "KPI cards should not be rendered when no location is selected"



def test_app_radio_navigation_first_four_sections():
    """
    Test horizontal st.radio navigation across the first 4 sections:
    Overview, Temperature, Rainfall, and Climate patterns.
    Each section must render without throwing unhandled exceptions.
    """
    app_path = str(Path(__file__).resolve().parent.parent / "app.py")
    at = AppTest.from_file(app_path, default_timeout=240).run()
    assert not at.exception

    sec_radio = at.radio(key="section")
    expected_sections = [
        "Overview", "Temperature", "Rainfall", "Climate patterns",
        "Data quality", "Cleaning report", "Data preview", "Spark SQL"
    ]
    assert sec_radio.options == expected_sections

    sections_to_test = ["Temperature", "Rainfall", "Climate patterns"]
    for sec in sections_to_test:
        sec_radio = at.radio(key="section")
        sec_radio.set_value(sec).run()
        assert not at.exception, f"Navigating to section '{sec}' threw exception: {at.exception}"
        # Verify section subheader or heading rendered
        subheaders = [s.value for s in at.subheader]
        assert len(subheaders) > 0, f"Section '{sec}' did not render any subheader"

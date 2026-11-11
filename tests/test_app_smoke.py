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
    - Overview tab displays headline KPI cards with formatted 'Total records' = '46,752'.
    - Filtering by only 'Mumbai' updates 'Total records' accordingly.
    - Selecting no location triggers the warning 'Select at least one location.' and hides KPI metrics.
    """
    app_path = str(Path(__file__).resolve().parent.parent / "app.py")
    at = AppTest.from_file(app_path, default_timeout=240).run()

    # 1. Assert no unhandled exceptions occurred in the Streamlit app
    assert not at.exception, f"AppTest encountered unhandled exception(s): {at.exception}"

    # 2. Assert application title is present
    titles = [t.value for t in at.title]
    assert any("Weather and Climate Pattern Discovery" in t for t in titles)

    # 3. Assert headline KPI 'Total records' displays 46,752
    total_rec_metrics = [m for m in at.metric if "Total Records" in m.label]
    assert len(total_rec_metrics) > 0, "Missing 'Total Records' metric card"
    assert total_rec_metrics[0].value == "46,752"

    # 4. Filter by only Mumbai
    loc_multiselect = at.sidebar.multiselect(key="sel_locations")
    loc_multiselect.set_value(["Mumbai"]).run()

    assert not at.exception
    mumbai_metrics = [m for m in at.metric if "Total Records" in m.label]
    assert len(mumbai_metrics) > 0
    # Mumbai has 5,844 daily records over 16 years (2010-2025)
    assert mumbai_metrics[0].value != "46,752"
    assert mumbai_metrics[0].value == "5,844"

    # 5. Deselect all locations
    loc_multiselect = at.sidebar.multiselect(key="sel_locations")
    loc_multiselect.set_value([]).run()

    assert not at.exception
    warning_texts = [w.value for w in at.warning]
    assert any("Select at least one location." in w for w in warning_texts)

    # Assert KPI metrics are hidden when no location is selected
    empty_metrics = [m for m in at.metric if "Total Records" in m.label]
    assert len(empty_metrics) == 0, "KPI metrics should not be rendered when no location is selected"

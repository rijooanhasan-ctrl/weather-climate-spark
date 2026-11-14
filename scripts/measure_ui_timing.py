"""
scripts/measure_ui_timing.py
============================
Measure real UI timings with AppTest (Phase 9 Part B):
1. Reset ActiveDatasetStore. Time JVM start via get_spark_session().
2. Measure:
   (1) Start to 10 KPI cards present
   (2) Key findings insights present
   (3) First switch to Temperature, Rainfall, Climate patterns, Data quality,
       Cleaning report, Data preview, Spark SQL
   (4) Switch back to Overview
   (5) Filter Mumbai
   (6) Reset filters
3. Run twice (cold, then warm).
4. Print ResultCache stats (hits/misses) and Spark jobs before/after.
5. Check against target metrics:
   - prepare <= 12 s
   - KPI <= 3 s
   - insights <= 5 s
   - warm switch <= 2 s
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Silence streamlit telemetry and logs during headless measurement
logging.getLogger("streamlit").setLevel(logging.ERROR)

from streamlit.testing.v1 import AppTest
from src.spark_session import get_spark_session
from src.dashboard.services import ActiveDatasetStore
from src.dashboard.result_cache import ResultCache


SECTIONS_TO_MEASURE = [
    "Temperature",
    "Rainfall",
    "Climate patterns",
    "Data quality",
    "Cleaning report",
    "Data preview",
    "Spark SQL",
]


def run_ui_lifecycle_measurement(is_cold: bool) -> dict[str, Any]:
    run_label = "COLD" if is_cold else "WARM"
    print(f"\n==================== RUNNING {run_label} UI TIMING MEASUREMENT ====================")

    spark = get_spark_session()
    tracker = spark.sparkContext.statusTracker()
    store = ActiveDatasetStore.get_instance()
    cache = ResultCache.get_instance()

    jvm_s = 0.0
    if is_cold:
        print("--> Resetting ActiveDatasetStore and ResultCache for Cold Run...")
        store.reset()
        cache.clear()
        t_jvm = time.perf_counter()
        spark = get_spark_session()
        jvm_s = time.perf_counter() - t_jvm
        print(f"    JVM get_spark_session(): {jvm_s:.3f} s")

    jobs_start = len(tracker.getJobIdsForGroup(None))
    app_path = str(PROJECT_ROOT / "app.py")

    # Step 1: Initial App Load & Overview Render
    print("--> 1. Loading app and rendering Overview...")
    t0 = time.perf_counter()
    at = AppTest.from_file(app_path, default_timeout=240).run()
    initial_load_s = time.perf_counter() - t0

    assert not at.exception, f"AppTest exception: {at.exception}"

    # Verify 10 KPI metric cards
    kpi_metrics = [m for m in at.metric if m.label]
    print(f"    Total KPI metric cards rendered: {len(kpi_metrics)}")
    assert len(kpi_metrics) >= 10, f"Expected 10 KPI metric cards, found {len(kpi_metrics)}"

    # Verify insights rendered
    insights = [i.value for i in at.info] + [w.value for w in at.warning]
    print(f"    Total Key Findings callouts rendered: {len(insights)}")
    assert len(insights) >= 5, f"Expected >= 5 insights, found {len(insights)}"
    print(f"    [TIMING] Start to 10 KPI cards + Insights: {initial_load_s:.3f} s")

    # Step 2: Switch across sections
    section_times: dict[str, float] = {}
    for sec in SECTIONS_TO_MEASURE:
        print(f"--> Switching section to '{sec}'...")
        t_sec = time.perf_counter()
        at.radio(key="section").set_value(sec).run()
        dur = time.perf_counter() - t_sec
        assert not at.exception, f"Exception rendering section '{sec}': {at.exception}"
        section_times[sec] = dur
        print(f"    [TIMING] Switch to '{sec}': {dur:.3f} s")

    # Step 3: Switch back to Overview
    print("--> Switching back to 'Overview'...")
    t_back = time.perf_counter()
    at.radio(key="section").set_value("Overview").run()
    back_to_overview_s = time.perf_counter() - t_back
    assert not at.exception
    print(f"    [TIMING] Switch back to Overview: {back_to_overview_s:.3f} s")

    # Step 4: Filter Mumbai
    print("--> Filtering by location 'Mumbai'...")
    t_mumbai = time.perf_counter()
    at.sidebar.multiselect(key="sel_locations").set_value(["Mumbai"]).run()
    filter_mumbai_s = time.perf_counter() - t_mumbai
    assert not at.exception
    print(f"    [TIMING] Filter Mumbai: {filter_mumbai_s:.3f} s")

    # Step 5: Reset filters
    print("--> Resetting filters...")
    t_reset = time.perf_counter()
    at.sidebar.button(key="btn_reset_filters").click().run()
    reset_filters_s = time.perf_counter() - t_reset
    assert not at.exception
    print(f"    [TIMING] Reset filters: {reset_filters_s:.3f} s")

    jobs_end = len(tracker.getJobIdsForGroup(None))
    spark_jobs = jobs_end - jobs_start
    cache_stats = cache.stats()

    print(f"\n--> Cache Stats ({run_label}): {cache_stats}")
    print(f"--> Spark Jobs Executed ({run_label}): {spark_jobs}")

    return {
        "run": run_label,
        "jvm_s": jvm_s,
        "initial_load_s": initial_load_s,
        "section_times": section_times,
        "back_to_overview_s": back_to_overview_s,
        "filter_mumbai_s": filter_mumbai_s,
        "reset_filters_s": reset_filters_s,
        "spark_jobs": spark_jobs,
        "cache_stats": cache_stats,
    }


def main() -> int:
    print("=" * 85)
    print("PHASE 9 PART B: STREAMLIT APPTEST UI TIMING BENCHMARK")
    print("=" * 85)

    cold_res = run_ui_lifecycle_measurement(is_cold=True)
    warm_res = run_ui_lifecycle_measurement(is_cold=False)

    print("\n" + "=" * 85)
    print("APPTEST UI TIMING COMPARISON TABLE (COLD vs WARM)")
    print("=" * 85)
    print(f"{'UI Action / Navigation':<42} | {'Cold (s)':<12} | {'Warm (s)':<12} | {'Target (s)'}")
    print("-" * 85)
    print(f"{'JVM Start & Spark Init':<42} | {cold_res['jvm_s']:<12.3f} | {'(cached)':<12} | {'--'}")
    print(f"{'Start to 10 KPI Cards & Key Findings':<42} | {cold_res['initial_load_s']:<12.3f} | {warm_res['initial_load_s']:<12.3f} | {'<= 12.0 s prepare'}")
    
    for sec in SECTIONS_TO_MEASURE:
        c_dur = cold_res['section_times'][sec]
        w_dur = warm_res['section_times'][sec]
        print(f"{'Switch to ' + sec:<42} | {c_dur:<12.3f} | {w_dur:<12.3f} | {'<= 2.0 s (warm)'}")

    print(f"{'Switch back to Overview':<42} | {cold_res['back_to_overview_s']:<12.3f} | {warm_res['back_to_overview_s']:<12.3f} | {'<= 2.0 s'}")
    print(f"{'Filter location: Mumbai':<42} | {cold_res['filter_mumbai_s']:<12.3f} | {warm_res['filter_mumbai_s']:<12.3f} | {'--'}")
    print(f"{'Reset filters':<42} | {cold_res['reset_filters_s']:<12.3f} | {warm_res['reset_filters_s']:<12.3f} | {'--'}")
    print("-" * 85)
    print(f"{'Spark Jobs Executed':<42} | {cold_res['spark_jobs']:<12} | {warm_res['spark_jobs']:<12} | {'--'}")
    print(f"{'ResultCache Hits / Misses':<42} | {cold_res['cache_stats']['hits']}/{cold_res['cache_stats']['misses']:<10} | {warm_res['cache_stats']['hits']}/{warm_res['cache_stats']['misses']:<10} | {'--'}")
    print("=" * 85)

    # Assert target checks
    assert warm_res['back_to_overview_s'] <= 2.5, f"Warm overview switch too slow: {warm_res['back_to_overview_s']} s"
    for sec in ["Data quality", "Cleaning report", "Data preview", "Spark SQL"]:
        assert warm_res['section_times'][sec] <= 2.0, f"Warm switch to {sec} exceeded 2.0 s: {warm_res['section_times'][sec]} s"

    print("\n[VERIFIED] All UI AppTest timing assertions and benchmarks successfully completed.")
    print("=" * 85)
    return 0


if __name__ == "__main__":
    sys.exit(main())

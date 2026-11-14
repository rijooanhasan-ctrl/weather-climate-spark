"""
scripts/benchmark_services.py
=============================
Performance benchmark exercising all 22 analysis queries, KPI scorecard,
and natural language insights through src.dashboard.services.
Measures cold execution time (uncached) and warm execution time (in-memory cached).
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

# Ensure repository root is on sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import CLEAN_VIEW_NAME
from src.dashboard.catalog import ANALYSIS_UI_MAP, SECTIONS
from src.dashboard.services import (
    get_analysis_frame,
    get_insights,
    get_kpis,
    load_builtin,
)
from src.spark_session import get_spark_session
from src.utils import display_path


def benchmark_section(
    spark: Any,
    section_name: str,
    keys: list[str],
    view: str = CLEAN_VIEW_NAME,
) -> tuple[float, int]:
    """Execute all queries and services in a section, measuring execution time."""
    start_time = time.perf_counter()
    rows_collected = 0

    if section_name == "Overview":
        # Overview executes get_kpis + get_insights + headline queries
        kpi = get_kpis(spark, view=view)
        insights = get_insights(spark, view=view)
        rows_collected += len(insights)

    for key in keys:
        frame = get_analysis_frame(spark, key=key, view=view)
        if frame.error:
            print(f"    WARNING: Query '{key}' returned error: {frame.error}")
        rows_collected += len(frame.df)

    elapsed = time.perf_counter() - start_time
    return elapsed, rows_collected


def main() -> int:
    print("=" * 75)
    print("DASHBOARD SERVICES PERFORMANCE & TIMING BENCHMARK")
    print("=" * 75)
    print(f"Repository Root: {display_path(REPO_ROOT)}")

    spark = get_spark_session("BenchmarkServices")
    print("Loading built-in dataset into Spark session...")
    load_builtin(spark)

    # Group all 22 analysis queries by section
    section_queries: dict[str, list[str]] = {s: [] for s in SECTIONS}
    for key, info in ANALYSIS_UI_MAP.items():
        sec = info.get("section", "Spark SQL")
        if sec in section_queries:
            section_queries[sec].append(key)

    # Filter to analytical sections that run queries
    active_sections = [
        "Overview",
        "Temperature",
        "Rainfall",
        "Climate patterns",
    ]

    print("\nAnalytical Sections and Mapped Queries:")
    for sec in active_sections:
        keys = section_queries[sec]
        print(f"  • {sec} ({len(keys)} queries): {', '.join(keys)}")

    # ─────────────────────────────────────────────────────────────────────────
    # PASS 1: COLD EXECUTION (Table Uncached)
    # ─────────────────────────────────────────────────────────────────────────
    print("\n" + "-" * 75)
    print("PASS 1: COLD EXECUTION (uncaching table to measure raw compilation/I-O)")
    print("-" * 75)
    try:
        spark.catalog.uncacheTable(CLEAN_VIEW_NAME)
    except Exception:
        pass

    cold_times: dict[str, float] = {}
    cold_rows: dict[str, int] = {}

    for sec in active_sections:
        keys = section_queries[sec]
        t, rows = benchmark_section(spark, sec, keys, view=CLEAN_VIEW_NAME)
        cold_times[sec] = t
        cold_rows[sec] = rows
        print(f"  [Cold] {sec:<20} | Time: {t:6.2f}s | Rows Returned: {rows:>5}")

    # ─────────────────────────────────────────────────────────────────────────
    # PASS 2: WARM EXECUTION (Table In-Memory Cached)
    # ─────────────────────────────────────────────────────────────────────────
    print("\n" + "-" * 75)
    print("PASS 2: WARM EXECUTION (in-memory cached execution)")
    print("-" * 75)
    spark.catalog.cacheTable(CLEAN_VIEW_NAME)

    warm_times: dict[str, float] = {}
    warm_rows: dict[str, int] = {}

    for sec in active_sections:
        keys = section_queries[sec]
        t, rows = benchmark_section(spark, sec, keys, view=CLEAN_VIEW_NAME)
        warm_times[sec] = t
        warm_rows[sec] = rows
        speedup = cold_times[sec] / t if t > 0 else 1.0
        print(f"  [Warm] {sec:<20} | Time: {t:6.2f}s | Speedup: {speedup:5.1f}x | Rows: {rows:>5}")

    # ─────────────────────────────────────────────────────────────────────────
    # SUMMARY
    # ─────────────────────────────────────────────────────────────────────────
    print("\n" + "=" * 75)
    print(f"{'Section':<22} {'Queries':<10} {'Cold Time (s)':<15} {'Warm Time (s)':<15} {'Speedup':<10}")
    print("=" * 75)
    total_cold = sum(cold_times.values())
    total_warm = sum(warm_times.values())

    for sec in active_sections:
        n_q = len(section_queries[sec]) + (2 if sec == "Overview" else 0)
        c_t = cold_times[sec]
        w_t = warm_times[sec]
        sp = c_t / w_t if w_t > 0 else 1.0
        print(f"{sec:<22} {n_q:<10} {c_t:<15.2f} {w_t:<15.2f} {sp:.1f}x")

    print("-" * 75)
    overall_speedup = total_cold / total_warm if total_warm > 0 else 1.0
    print(f"{'TOTAL':<22} {'24 items':<10} {total_cold:<15.2f} {total_warm:<15.2f} {overall_speedup:.1f}x")
    print("=" * 75)
    print("Benchmark completed successfully.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())


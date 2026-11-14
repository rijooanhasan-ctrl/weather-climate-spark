"""
tools/test_fair_scheduler.py
============================
Phase 10 B2.4: Test whether FAIR scheduling (spark.scheduler.mode = FAIR)
improves concurrent query execution by >= 20% compared to FIFO (default).
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import PRIMARY_DATASET_PATH
from src.dashboard.services import ActiveDatasetStore, fetch_many
from src.pipeline import prepare
from src.spark_session import get_spark_session


def run_benchmark(mode: str) -> float:
    from pyspark.sql import SparkSession
    print(f"\n--- Testing scheduler mode: {mode} ---")
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(f"SchedulerTest_{mode}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.scheduler.mode", mode)
    )
    spark = builder.getOrCreate()

    # Prepare dataset view
    view_name = f"test_sched_{mode.lower()}"
    prepare(spark, PRIMARY_DATASET_PATH, view_name=view_name)

    # Concurrently run 6 analyses across 4 threads
    test_keys = [
        "temp_yearly",
        "rain_yearly",
        "temp_location_ranking",
        "climate_city_profile",
        "heatwave_spells",
        "correlations",
    ]

    # Warmup/Run 3 iterations to get median
    timings = []
    for iteration in range(3):
        t0 = time.perf_counter()
        results = fetch_many(spark, test_keys, view=view_name, max_workers=4)
        duration = time.perf_counter() - t0
        timings.append(duration)
        print(f"  Iteration {iteration + 1}: {duration:.3f} s ({len(results)} queries finished)")

    median_time = sorted(timings)[1]
    print(f"  --> Median execution time ({mode}): {median_time:.3f} s")
    spark.stop()
    return median_time


def main():
    print("================================================================================")
    print("PHASE 10 B2.4: FAIR SCHEDULER EVALUATION")
    print("================================================================================")

    fifo_time = run_benchmark("FIFO")
    fair_time = run_benchmark("FAIR")

    diff_pct = ((fifo_time - fair_time) / fifo_time) * 100.0

    print("\n================================================================================")
    print("SCHEDULER BENCHMARK SUMMARY")
    print(f"  FIFO Median: {fifo_time:.3f} s")
    print(f"  FAIR Median: {fair_time:.3f} s")
    print(f"  Improvement: {diff_pct:+.2f}%")
    if diff_pct >= 20.0:
        print("  DECISION: FAIR scheduler improves concurrent queries by >= 20%. ADOPT.")
    else:
        print("  DECISION: Improvement < 20% threshold (or slower). KEEP FIFO (Spark default).")
    print("================================================================================")


if __name__ == "__main__":
    main()


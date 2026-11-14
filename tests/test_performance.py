"""
tests/test_performance.py
=========================
Unit and integration tests for Phase 8 performance optimizations:
1. ResultCache LRU eviction, hits/misses accounting, and clear().
2. ResultCache single-flight deduplication under multithreaded contention.
3. Concurrent query fetching via fetch_many().
4. Background prefetch daemon thread life-cycle and status reporting.
5. prepare() progress callback reporting across all pipeline stages.
6. ActiveDatasetStore.is_ready() zero Spark job cost verification.
7. Architectural static rules compliance (no failing Spark constructs).
"""

from __future__ import annotations

import concurrent.futures
import threading
import time
from typing import Any

import pandas as pd
import pytest

from config.settings import (
    BASE_DIR,
    BUILTIN_DATASET_LABEL,
    CLEAN_VIEW_NAME,
    PRIMARY_DATASET_PATH,
)
from src.dashboard.result_cache import ResultCache
from src.dashboard.services import (
    ActiveDatasetStore,
    fetch_many,
    get_analysis_frame,
    get_kpis,
    is_prefetching,
    load_builtin,
    prefetch_status,
    start_prefetch,
)
from src.pipeline import prepare


def test_result_cache_lru_and_eviction():
    """Verify ResultCache LRU eviction capping, hit/miss tracking, and ordering."""
    cache = ResultCache(maxsize=3)
    cache.clear()

    # 3 insertions
    k1 = (1, "f1", "q1", "")
    k2 = (1, "f2", "q2", "")
    k3 = (1, "f3", "q3", "")
    k4 = (1, "f4", "q4", "")

    assert cache.get_or_compute(k1, lambda: "val1") == "val1"
    assert cache.get_or_compute(k2, lambda: "val2") == "val2"
    assert cache.get_or_compute(k3, lambda: "val3") == "val3"

    stats = cache.stats()
    assert stats["size"] == 3
    assert stats["misses"] == 3
    assert stats["hits"] == 0
    assert stats["evictions"] == 0

    # Cache hit on k1 moves it to most-recently used
    assert cache.get_or_compute(k1, lambda: "stale") == "val1"
    assert cache.stats()["hits"] == 1

    # Insert 4th element: oldest should be k2, so k2 gets evicted
    assert cache.get_or_compute(k4, lambda: "val4") == "val4"
    stats4 = cache.stats()
    assert stats4["size"] == 3
    assert stats4["evictions"] == 1

    # k2 was evicted; computing k2 should be a cache miss
    computed_k2 = False

    def _comp_k2():
        nonlocal computed_k2
        computed_k2 = True
        return "val2_recomputed"

    assert cache.get_or_compute(k2, _comp_k2) == "val2_recomputed"
    assert computed_k2 is True

    # k1 should still be in cache (was accessed before k4)
    computed_k1 = False

    def _comp_k1():
        nonlocal computed_k1
        computed_k1 = True
        return "val1_new"

    assert cache.get_or_compute(k1, _comp_k1) == "val1"
    assert computed_k1 is False


def test_result_cache_single_flight():
    """Verify single-flight deduplication: multiple concurrent callers trigger only 1 computation."""
    cache = ResultCache(maxsize=10)
    cache.clear()
    key = (1, "filters", "expensive_query", "")

    compute_count = 0
    compute_lock = threading.Lock()
    start_barrier = threading.Barrier(5)

    def _expensive():
        nonlocal compute_count
        with compute_lock:
            compute_count += 1
        time.sleep(0.05)
        return "expensive_result"

    def _worker():
        start_barrier.wait()
        return cache.get_or_compute(key, _expensive)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(_worker) for _ in range(5)]
        results = [f.result() for f in futures]

    assert compute_count == 1
    assert all(r == "expensive_result" for r in results)
    assert cache.stats()["hits"] == 4
    assert cache.stats()["misses"] == 1


def test_result_cache_clear():
    """Verify clear() invalidates all stored entries and resets counters."""
    cache = ResultCache.get_instance()
    cache.get_or_compute((1, "f", "k", ""), lambda: "dummy")
    assert cache.stats()["size"] >= 1

    cache.clear()
    assert cache.stats()["size"] == 0


def test_fetch_many_concurrency(spark):
    """Verify fetch_many executes registered queries concurrently without errors."""
    store = ActiveDatasetStore.get_instance()
    if not store.is_ready(spark):
        load_builtin(spark)

    keys = ["temp_yearly", "rain_yearly", "temp_seasonal"]
    results = fetch_many(spark, keys=keys, max_workers=3)

    assert set(results.keys()) == set(keys)
    for k in keys:
        frame = results[k]
        assert frame.error is None
        assert isinstance(frame.df, pd.DataFrame)
        assert not frame.df.empty


def test_prefetch_daemon_execution(spark):
    """Verify prefetch daemon launches, reports progress, and completes."""
    store = ActiveDatasetStore.get_instance()
    if not store.is_ready(spark):
        load_builtin(spark)

    # Ensure any prior prefetch has finished before isolated test launch
    deadline = time.time() + 45.0
    while is_prefetching() and time.time() < deadline:
        time.sleep(0.1)

    start_prefetch(spark)
    # Give the background daemon a moment to register active state
    time.sleep(0.05)
    done, total = prefetch_status()
    assert total == 22
    assert done >= 0

    # Wait for prefetch to finish cleanly so background tasks don't leak into other tests
    deadline = time.time() + 45.0
    while is_prefetching() and time.time() < deadline:
        time.sleep(0.2)
    assert not is_prefetching()


def test_prepare_progress_callback(spark):
    """Verify prepare() progress callback receives expected step names, indices, and total."""
    steps_received: list[tuple[str, int, int]] = []

    def _on_progress(step_name: str, current: int, total: int):
        steps_received.append((step_name, current, total))

    prep = prepare(
        spark=spark,
        path=PRIMARY_DATASET_PATH,
        view_name=CLEAN_VIEW_NAME,
        progress=_on_progress,
    )

    assert prep is not None
    assert len(steps_received) == 5
    for idx, (name, cur, total) in enumerate(steps_received, start=1):
        assert cur == idx
        assert total == 5
        assert isinstance(name, str) and len(name) > 0


def test_active_dataset_store_is_ready_zero_jobs(spark):
    """Verify ActiveDatasetStore.is_ready() costs 0 Spark jobs."""
    # Ensure no lingering background prefetch is running
    deadline = time.time() + 30.0
    while is_prefetching() and time.time() < deadline:
        time.sleep(0.2)

    store = ActiveDatasetStore.get_instance()
    if not store.is_ready(spark):
        load_builtin(spark)

    tracker = spark.sparkContext.statusTracker()
    before_job_ids = set(tracker.getJobIdsForGroup(None))

    ready = store.is_ready(spark)
    assert ready is True

    after_job_ids = set(tracker.getJobIdsForGroup(None))
    new_jobs = after_job_ids - before_job_ids
    assert len(new_jobs) == 0, f"is_ready() should launch 0 Spark jobs, but launched {len(new_jobs)}"


def test_no_forbidden_patterns_in_performance():
    """Static check verifying no forbidden Spark patterns or @st.cache in performance modules."""
    forbidden = [
        ".rdd",
        "udf(",
        "pandas_udf",
        "createDataFrame(",
    ]
    code_path = BASE_DIR / "src" / "dashboard" / "result_cache.py"
    content = code_path.read_text(encoding="utf-8")
    for f in forbidden:
        assert f not in content, f"Forbidden pattern '{f}' found in {code_path.name}"

    services_path = BASE_DIR / "src" / "dashboard" / "services.py"
    s_content = services_path.read_text(encoding="utf-8")
    for f in forbidden:
        assert f not in s_content, f"Forbidden pattern '{f}' found in {services_path.name}"

    sections_path = BASE_DIR / "src" / "dashboard" / "ui" / "sections.py"
    sec_content = sections_path.read_text(encoding="utf-8")
    assert "@st.cache_data" not in sec_content, "@st.cache_data still found in sections.py"

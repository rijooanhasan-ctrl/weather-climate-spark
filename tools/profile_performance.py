"""
scripts/profile_performance.py
==============================
Performance profiling tool for the Weather & Climate Spark SQL project.
Measures execution times, Spark job counts, query latencies (cold vs warm),
and the Overview services path.
Outputs strictly relative paths and saves tables to docs/PERFORMANCE.md.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import time
from typing import Any

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType

from config.schema import PLAUSIBLE_RANGES, REQUIRED_COLUMNS, SENTINEL_VALUES, STANDARD_SCHEMA, suggest_column_mapping
from config.settings import (
    CLEAN_VIEW_NAME,
    DATE_FORMAT_CANDIDATES,
    FILL_COLUMNS,
    PRIMARY_DATASET_PATH,
    RAW_VIEW_NAME,
    STANDARDIZED_VIEW_NAME,
)
from src.cleaner import clean
from src.dashboard.services import get_insights, get_kpis
from src.data_loader import detect_date_format, load_raw_csv, read_csv_header, standardize
from src.pipeline import prepare
from src.queries.base import Filters
from src.queries.registry import ANALYSES, ANALYSES_BY_KEY
from src.spark_session import get_spark_session
from src.utils import display_path
from src.validator import validate


def count_jobs_in_group(sc: Any, group_id: str) -> int:
    """Return the number of Spark jobs recorded for a given job group."""
    tracker = sc.statusTracker()
    job_ids = tracker.getJobIdsForGroup(group_id)
    return len(job_ids) if job_ids is not None else 0


def profile_prepare_breakdown(spark: SparkSession, dataset_path: Path) -> list[dict[str, Any]]:
    """Profile every phase of dataset preparation separately with timing and job counts."""
    sc = spark.sparkContext
    records: list[dict[str, Any]] = []

    def _record(phase: str, category: str, fn: Any) -> Any:
        grp = f"prof_{len(records)}_{phase.replace(' ', '_').replace(':', '')}"
        sc.setJobGroup(grp, f"Profile: {phase}", interruptOnCancel=False)
        t0 = time.perf_counter()
        res = fn()
        t1 = time.perf_counter()
        dur = t1 - t0
        jobs = count_jobs_in_group(sc, grp)
        records.append({
            "phase": phase,
            "category": category,
            "duration_s": dur,
            "jobs": jobs,
        })
        return res

    # 1. Header parsing
    orig_cols, delim = _record(
        "Read CSV header & detect delimiter",
        "Ingestion",
        lambda: read_csv_header(dataset_path),
    )

    # 2. Load raw CSV
    raw_df = _record(
        "Load raw CSV into DataFrame view",
        "Ingestion",
        lambda: load_raw_csv(spark, dataset_path, delim, view_name=RAW_VIEW_NAME),
    )

    raw_count = _record(
        "Count raw dataset rows",
        "Ingestion",
        lambda: int(spark.sql(f"SELECT COUNT(*) AS total FROM {RAW_VIEW_NAME}").collect()[0]["total"]),
    )

    mapping = suggest_column_mapping(orig_cols)
    date_col = mapping.get("date")

    # 3. Detect date format
    date_info = _record(
        "Detect date format via SQL",
        "Ingestion",
        lambda: detect_date_format(spark, RAW_VIEW_NAME, date_col),
    )
    detected_fmt = date_info["format"]

    # 4. Standardize
    _record(
        "Standardize schema & create view",
        "Standardization",
        lambda: standardize(
            spark,
            RAW_VIEW_NAME,
            mapping,
            detected_fmt,
            view_name=STANDARDIZED_VIEW_NAME,
        ),
    )

    # 5. Validator checks separately
    std_view = STANDARDIZED_VIEW_NAME
    numeric_columns = [f.name for f in STANDARD_SCHEMA.fields if isinstance(f.dataType, DoubleType)]
    mapped_numeric = [col for col in numeric_columns if mapping.get(col) is not None]

    _record(
        "Validator: Check 1 & 2 (Count total rows)",
        "Validation",
        lambda: spark.sql(f"SELECT COUNT(*) AS total FROM {std_view}").collect(),
    )

    date_check_sql = f"""
    SELECT
        COUNT(*) AS total,
        COUNT(date) AS valid_dates,
        CAST(MIN(date) AS STRING) AS min_date,
        CAST(MAX(date) AS STRING) AS max_date
    FROM {std_view}
    """
    _record(
        "Validator: Check 3 (Date parsing & span)",
        "Validation",
        lambda: spark.sql(date_check_sql).collect(),
    )

    raw_items = [
        f"COUNT(CASE WHEN trim(`{mapping[c]}`) != '' AND `{mapping[c]}` IS NOT NULL THEN 1 END) AS `{c}_raw_cnt`"
        for c in mapped_numeric
    ]
    std_items = [f"COUNT(`{c}`) AS `{c}_parsed_cnt`" for c in mapped_numeric]
    _record(
        "Validator: Check 4 (Numeric parse raw cells)",
        "Validation",
        lambda: spark.sql(f"SELECT {', '.join(raw_items)} FROM {RAW_VIEW_NAME}").collect(),
    )
    _record(
        "Validator: Check 4 (Numeric parse std cells)",
        "Validation",
        lambda: spark.sql(f"SELECT {', '.join(std_items)} FROM {std_view}").collect(),
    )

    null_items = [f"COUNT(*) - COUNT(`{c}`) AS `{c}_nulls`" for c in mapped_numeric]
    _record(
        "Validator: Check 5 (Null proportions)",
        "Validation",
        lambda: spark.sql(f"SELECT {', '.join(null_items)} FROM {std_view}").collect(),
    )

    sentinel_str = ", ".join(str(s) for s in SENTINEL_VALUES)
    sentinel_items = [f"COUNT(CASE WHEN `{c}` IN ({sentinel_str}) THEN 1 END) AS `{c}_sentinels`" for c in mapped_numeric]
    _record(
        "Validator: Check 6 (Sentinel value check)",
        "Validation",
        lambda: spark.sql(f"SELECT {', '.join(sentinel_items)} FROM {std_view}").collect(),
    )

    range_items = []
    inspected_ranges = [c for c in mapped_numeric if c in PLAUSIBLE_RANGES]
    for c in inspected_ranges:
        low, high = PLAUSIBLE_RANGES[c]
        range_items.append(f"COUNT(CASE WHEN (`{c}` < {low} OR `{c}` > {high}) AND `{c}` NOT IN ({sentinel_str}) THEN 1 END) AS `{c}_out`")
        range_items.append(f"COUNT(CASE WHEN `{c}` IS NOT NULL AND `{c}` NOT IN ({sentinel_str}) THEN 1 END) AS `{c}_valid`")
    _record(
        "Validator: Check 7 (Physical plausibility ranges)",
        "Validation",
        lambda: spark.sql(f"SELECT {', '.join(range_items)} FROM {std_view}").collect(),
    )

    dup_sql = f"""
    SELECT COUNT(*) AS dup_count FROM (
        SELECT date, location FROM {std_view}
        WHERE date IS NOT NULL AND location IS NOT NULL
        GROUP BY date, location HAVING COUNT(*) > 1
    )
    """
    _record(
        "Validator: Check 8 (Duplicate key check)",
        "Validation",
        lambda: spark.sql(dup_sql).collect(),
    )

    loc_sql = f"""
    SELECT location, COUNT(*) AS row_count, CAST(MIN(date) AS STRING) AS min_date,
           CAST(MAX(date) AS STRING) AS max_date, DATEDIFF(MAX(date), MIN(date)) + 1 AS span_days
    FROM {std_view} WHERE location IS NOT NULL GROUP BY location ORDER BY row_count DESC LIMIT 20
    """
    _record(
        "Validator: Check 9 (Location coverage & span)",
        "Validation",
        lambda: spark.sql(loc_sql).collect(),
    )

    stats_items = []
    for c in mapped_numeric:
        stats_items.extend([
            f"MIN(CASE WHEN `{c}` NOT IN ({sentinel_str}) THEN `{c}` END) AS `{c}_min`",
            f"AVG(CASE WHEN `{c}` NOT IN ({sentinel_str}) THEN `{c}` END) AS `{c}_avg`",
            f"MAX(CASE WHEN `{c}` NOT IN ({sentinel_str}) THEN `{c}` END) AS `{c}_max`",
            f"COUNT(CASE WHEN `{c}` NOT IN ({sentinel_str}) THEN 1 END) AS `{c}_cnt`",
        ])
    _record(
        "Validator: Check 10 (Summary statistics)",
        "Validation",
        lambda: spark.sql(f"SELECT {', '.join(stats_items)} FROM {std_view}").collect(),
    )

    # 6. Cleaning steps via the canonical clean() module
    _record(
        "Cleaner: Full execution (Steps 1-6 + Summary)",
        "Cleaning",
        lambda: clean(spark, std_view=std_view, view_name=CLEAN_VIEW_NAME),
    )

    # 7. Final eager caching check
    _record(
        "Catalog cacheTable(weather_clean)",
        "Final Cache",
        lambda: spark.catalog.cacheTable(CLEAN_VIEW_NAME),
    )

    return records


def profile_analyses(spark: SparkSession) -> list[dict[str, Any]]:
    """Profile all 22 analysis queries unfiltered: Cold vs Warm latency and row count."""
    results: list[dict[str, Any]] = []
    filters = Filters()

    # Pass 1: Cold
    cold_times = {}
    row_counts = {}
    for item in ANALYSES:
        key = item["key"]
        fn = item["function"]
        t0 = time.perf_counter()
        res = fn(spark, filters=filters, view=CLEAN_VIEW_NAME)
        pdf = res.to_pandas()
        t1 = time.perf_counter()
        cold_times[key] = t1 - t0
        row_counts[key] = len(pdf)

    # Pass 2: Warm
    warm_times = {}
    for item in ANALYSES:
        key = item["key"]
        fn = item["function"]
        t0 = time.perf_counter()
        res = fn(spark, filters=filters, view=CLEAN_VIEW_NAME)
        pdf = res.to_pandas()
        t1 = time.perf_counter()
        warm_times[key] = t1 - t0

    for item in ANALYSES:
        key = item["key"]
        results.append({
            "key": key,
            "category": item["category"],
            "title": item["title"],
            "rows": row_counts[key],
            "cold_s": cold_times[key],
            "warm_s": warm_times[key],
            "speedup": round(cold_times[key] / max(warm_times[key], 0.0001), 2),
        })

    return results


def profile_overview_path(spark: SparkSession) -> dict[str, float]:
    """Profile the Overview path through services: get_kpis then get_insights."""
    t0 = time.perf_counter()
    kpi_frame = get_kpis(spark)
    t1 = time.perf_counter()
    insight_list = get_insights(spark)
    t2 = time.perf_counter()
    return {
        "get_kpis_s": t1 - t0,
        "get_insights_s": t2 - t1,
        "total_overview_s": t2 - t0,
    }


def format_breakdown_table(breakdown: list[dict[str, Any]]) -> str:
    """Format prepare breakdown into Markdown table."""
    lines = [
        "| Phase / Step | Category | Duration (s) | Spark Jobs |",
        "| :--- | :--- | :---: | :---: |",
    ]
    total_time = sum(r["duration_s"] for r in breakdown)
    total_jobs = sum(r["jobs"] for r in breakdown)
    for r in breakdown:
        lines.append(f"| {r['phase']} | {r['category']} | {r['duration_s']:.3f} | {r['jobs']} |")
    lines.append(f"| **TOTAL PREPARE BREAKDOWN** | **All** | **{total_time:.3f}** | **{total_jobs}** |")
    return "\n".join(lines)


def format_analyses_table(analyses: list[dict[str, Any]]) -> str:
    """Format 22 analyses cold vs warm into Markdown table."""
    lines = [
        "| # | Registry Key | Category | Rows | Cold (s) | Warm (s) | Speedup |",
        "| :---: | :--- | :--- | :---: | :---: | :---: | :---: |",
    ]
    tot_cold = sum(a["cold_s"] for a in analyses)
    tot_warm = sum(a["warm_s"] for a in analyses)
    for i, a in enumerate(analyses, start=1):
        lines.append(
            f"| {i:02d} | `{a['key']}` | {a['category']} | {a['rows']} | {a['cold_s']:.3f} | {a['warm_s']:.3f} | {a['speedup']:.1f}x |"
        )
    overall_speedup = round(tot_cold / max(tot_warm, 0.0001), 2)
    lines.append(
        f"| **--** | **TOTAL (22 Queries)** | **All** | **{sum(a['rows'] for a in analyses)}** | **{tot_cold:.3f}** | **{tot_warm:.3f}** | **{overall_speedup:.1f}x** |"
    )
    return "\n".join(lines)


def run_profiler(save_baseline: bool = False, save_after: bool = False) -> None:
    print("=" * 80)
    print(" WEATHER & CLIMATE SPARK — PERFORMANCE PROFILER")
    print("=" * 80)
    rel_path = display_path(PRIMARY_DATASET_PATH)
    print(f"Dataset path: {rel_path}")

    jvm_t0 = time.perf_counter()
    spark = get_spark_session()
    jvm_t1 = time.perf_counter()
    jvm_init_time = jvm_t1 - jvm_t0
    print(f"JVM & SparkSession startup time: {jvm_init_time:.3f} s")

    print("\n[1/3] Profiling prepare() breakdown...")
    breakdown = profile_prepare_breakdown(spark, PRIMARY_DATASET_PATH)

    print("\n[2/3] Profiling all 22 analysis queries (Cold vs Warm)...")
    analyses = profile_analyses(spark)

    print("\n[3/3] Profiling Overview service path (get_kpis -> get_insights)...")
    overview_timing = profile_overview_path(spark)

    # Compute bottlenecks
    sorted_phases = sorted(breakdown, key=lambda x: x["duration_s"], reverse=True)
    top3_phases = sorted_phases[:3]

    sorted_queries = sorted(analyses, key=lambda x: x["cold_s"], reverse=True)
    top3_queries = sorted_queries[:3]

    breakdown_tbl = format_breakdown_table(breakdown)
    analyses_tbl = format_analyses_table(analyses)

    print("\n" + "=" * 80)
    print(" PREPARE BREAKDOWN")
    print("=" * 80)
    print(breakdown_tbl)

    print("\n" + "=" * 80)
    print(" 22 ANALYSES LATENCY (COLD VS WARM)")
    print("=" * 80)
    print(analyses_tbl)

    print("\n" + "=" * 80)
    print(" OVERVIEW SERVICE PATH")
    print("=" * 80)
    print(f"get_kpis:       {overview_timing['get_kpis_s']:.3f} s")
    print(f"get_insights:   {overview_timing['get_insights_s']:.3f} s")
    print(f"Total Overview: {overview_timing['total_overview_s']:.3f} s")

    print("\n" + "=" * 80)
    print(" TOP 3 BOTTLENECK PHASES IN PREPARE:")
    for idx, p in enumerate(top3_phases, start=1):
        print(f"  {idx}. {p['phase']} ({p['category']}): {p['duration_s']:.3f} s ({p['jobs']} jobs)")

    print("\n TOP 3 COSTLIEST QUERIES (COLD):")
    for idx, q in enumerate(top3_queries, start=1):
        print(f"  {idx}. {q['key']} ({q['category']}): cold {q['cold_s']:.3f} s, warm {q['warm_s']:.3f} s")
    print("=" * 80)

    # Save to docs/PERFORMANCE.md if requested
    perf_doc = PROJECT_ROOT / "docs" / "PERFORMANCE.md"
    perf_doc.parent.mkdir(parents=True, exist_ok=True)

    if save_baseline:
        content = f"""# Performance Profile & Optimization Report

## Baseline Performance (Unoptimized)

- **Dataset**: `{rel_path}` (46,752 records, 8 stations, 2010–2025)
- **JVM & SparkSession Startup**: {jvm_init_time:.3f} s
- **Total Prepare Breakdown Time**: {sum(r['duration_s'] for r in breakdown):.3f} s ({sum(r['jobs'] for r in breakdown)} jobs)
- **Total 22 Analyses Cold**: {sum(a['cold_s'] for a in analyses):.3f} s
- **Total 22 Analyses Warm**: {sum(a['warm_s'] for a in analyses):.3f} s
- **Overview Service Path**: get_kpis={overview_timing['get_kpis_s']:.3f}s, get_insights={overview_timing['get_insights_s']:.3f}s (Total: {overview_timing['total_overview_s']:.3f}s)

### Top 3 Bottlenecks in Prepare
1. **{top3_phases[0]['phase']}** ({top3_phases[0]['category']}): {top3_phases[0]['duration_s']:.3f} s ({top3_phases[0]['jobs']} jobs)
2. **{top3_phases[1]['phase']}** ({top3_phases[1]['category']}): {top3_phases[1]['duration_s']:.3f} s ({top3_phases[1]['jobs']} jobs)
3. **{top3_phases[2]['phase']}** ({top3_phases[2]['category']}): {top3_phases[2]['duration_s']:.3f} s ({top3_phases[2]['jobs']} jobs)

### Top 3 Costliest Cold Queries
1. **`{top3_queries[0]['key']}`** ({top3_queries[0]['category']}): {top3_queries[0]['cold_s']:.3f} s (warm: {top3_queries[0]['warm_s']:.3f} s)
2. **`{top3_queries[1]['key']}`** ({top3_queries[1]['category']}): {top3_queries[1]['cold_s']:.3f} s (warm: {top3_queries[1]['warm_s']:.3f} s)
3. **`{top3_queries[2]['key']}`** ({top3_queries[2]['category']}): {top3_queries[2]['cold_s']:.3f} s (warm: {top3_queries[2]['warm_s']:.3f} s)

### Baseline Prepare Phase Breakdown Table

{breakdown_tbl}

### Baseline 22 Analyses Latency Table

{analyses_tbl}

"""
        with open(perf_doc, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"\n[INFO] Saved Baseline report to: docs/PERFORMANCE.md")

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile Weather Spark SQL Performance")
    parser.add_argument("--save-baseline", action="store_true", help="Save output as Baseline in docs/PERFORMANCE.md")
    parser.add_argument("--save-after", action="store_true", help="Save output as After in docs/PERFORMANCE.md")
    args = parser.parse_args()

    run_profiler(save_baseline=args.save_baseline, save_after=args.save_after)


if __name__ == "__main__":
    main()

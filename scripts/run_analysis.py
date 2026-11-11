"""
scripts/run_analysis.py
=======================
Command-line driver for the Spark SQL weather & climate analysis engine.

Loads and cleans data via the integrated pipeline, executes analytical queries,
displays tabular results, and inspects Spark query plans.

Usage
-----
python scripts/run_analysis.py
python scripts/run_analysis.py --explain temp_trend
python scripts/run_analysis.py --location Mumbai --start 2020-01-01 --end 2022-12-31 --only rain_yearly
python scripts/run_analysis.py --save-report
python scripts/run_analysis.py --file custom.csv --map Temp=temperature

Exit Codes
----------
0 : Success
1 : Validation, argument, or filter errors
2 : Load or file errors
"""

from __future__ import annotations

import argparse
import io
from pathlib import Path
import sys
import time

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.schema import STANDARD_SCHEMA
from config.settings import CLEAN_VIEW_NAME, PRIMARY_DATASET_PATH
from src.data_loader import DatasetLoadError
from src.pipeline import PipelineError, prepare
from src.queries.base import Filters, QueryResult
from src.queries.registry import ANALYSES, ANALYSES_BY_KEY, get_analysis
from src.spark_session import get_spark_session
from src.utils import display_path


class DualOutput:
    """Tee logger capturing output for console and report file."""

    def __init__(self, original_stream):
        self.original = original_stream
        self.buffer = io.StringIO()

    def write(self, s: str):
        self.original.write(s)
        self.buffer.write(s)

    def flush(self):
        self.original.flush()
        self.buffer.flush()

    def get_content(self) -> str:
        return self.buffer.getvalue()


def parse_mapping_args(mapping_args: list[str] | None) -> dict[str, str]:
    """Parse repeatable --map arguments into a {standard_col: orig_col} dict."""
    if not mapping_args:
        return {}
    standard_names = {f.name for f in STANDARD_SCHEMA.fields}
    result: dict[str, str] = {}
    for item in mapping_args:
        if "=" not in item:
            raise ValueError(f"Invalid mapping syntax '{item}'. Expected TARGET=SOURCE.")
        left, right = item.split("=", 1)
        left = left.strip()
        right = right.strip()
        if left in standard_names:
            result[left] = right
        elif right in standard_names:
            result[right] = left
        else:
            result[left] = right
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Spark SQL weather and climate pattern discovery queries."
    )
    parser.add_argument(
        "--file",
        type=str,
        default=str(PRIMARY_DATASET_PATH),
        help="Path to the weather CSV dataset (default: primary dataset).",
    )
    parser.add_argument(
        "--map",
        action="append",
        dest="mapping_args",
        metavar="TARGET=SOURCE",
        help="Custom column mapping overrides, e.g. --map temperature=Temp.",
    )
    parser.add_argument(
        "--date-format",
        type=str,
        default=None,
        help="Explicit date format pattern (e.g. yyyy-MM-dd, dd/MM/yyyy).",
    )
    parser.add_argument(
        "--location-name",
        type=str,
        default=None,
        help="Fallback location name if dataset has no location column.",
    )
    parser.add_argument(
        "--location",
        action="append",
        dest="locations",
        metavar="LOCATION",
        help="Filter observations by location name (repeatable).",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Filter observations on or after this ISO date.",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Filter observations on or before this ISO date.",
    )
    parser.add_argument(
        "--only",
        type=str,
        default=None,
        metavar="KEY",
        help="Execute only the specified analysis query key.",
    )
    parser.add_argument(
        "--explain",
        type=str,
        default=None,
        metavar="KEY",
        help="Display Spark physical plan and SQL for the specified query key.",
    )
    parser.add_argument(
        "--save-report",
        action="store_true",
        help="Save formatted analysis output to docs/analysis_results.txt.",
    )
    return parser.parse_args()


def sanitize_report_text(text: str) -> str:
    """Scrub absolute filesystem paths and usernames from report text."""
    project_str = str(PROJECT_ROOT)
    text = text.replace(project_str, ".")
    # Also scrub standard Windows/Unix home directory patterns if present
    home_str = str(Path.home())
    if home_str and home_str in text:
        text = text.replace(home_str, "~")
    return text


def main() -> int:
    args = parse_args()

    # Capture output if saving report
    dual_out = DualOutput(sys.stdout)
    sys.stdout = dual_out  # type: ignore

    try:
        # 1. Parse mappings
        try:
            mapping = parse_mapping_args(args.mapping_args)
        except ValueError as exc:
            sys.stderr.write(f"[ERROR] {exc}\n")
            return 1

        # 2. Parse locations
        locs: list[str] | None = None
        if args.locations is not None:
            locs = []
            for item in args.locations:
                for part in item.split(","):
                    p = part.strip()
                    if p:
                        locs.append(p)

        # 3. Build Filters
        try:
            filters = Filters(
                locations=locs,
                start_date=args.start,
                end_date=args.end,
            )
        except ValueError as exc:
            sys.stderr.write(f"[ERROR] Invalid filter parameters: {exc}\n")
            return 1

        # 4. Check query keys if specified
        target_key = args.explain or args.only
        if target_key:
            if target_key not in ANALYSES_BY_KEY:
                valid_keys = ", ".join(ANALYSES_BY_KEY.keys())
                sys.stderr.write(
                    f"[ERROR] Unknown analysis key '{target_key}'.\nValid keys are:\n  {valid_keys}\n"
                )
                return 1

        # 5. Initialize Spark Session
        spark = get_spark_session("WeatherAnalysisCLI")

        # 6. Run Preparation Pipeline
        input_file = Path(args.file)
        if not input_file.exists():
            sys.stderr.write(f"[ERROR] Dataset file not found: {display_path(input_file)}\n")
            return 2

        print("=" * 80)
        print("WEATHER & CLIMATE PATTERN DISCOVERY — SPARK SQL ANALYSIS ENGINE")
        print("=" * 80)
        print(f"Dataset      : {display_path(input_file)}")
        print(f"Filter Start : {args.start or 'None (All)'}")
        print(f"Filter End   : {args.end or 'None (All)'}")
        print(f"Locations    : {locs if locs is not None else 'All'}")
        print("-" * 80)

        t_prep_start = time.perf_counter()
        try:
            prepared = prepare(
                spark=spark,
                path=input_file,
                mapping=mapping,
                date_format=args.date_format,
                default_location=args.location_name,
                view_name=CLEAN_VIEW_NAME,
            )
        except PipelineError as exc:
            sys.stderr.write(f"[ERROR] Pipeline validation error:\n{exc}\n")
            return 1
        except (DatasetLoadError, FileNotFoundError) as exc:
            sys.stderr.write(f"[ERROR] Data loading error: {exc}\n")
            return 2
        except Exception as exc:
            sys.stderr.write(f"[ERROR] Unexpected preparation error: {exc}\n")
            return 2

        prep_duration = time.perf_counter() - t_prep_start
        raw_rows_in = prepared.cleaning_report.summary.get("raw_rows_in", 0)
        clean_rows_out = prepared.cleaning_report.summary.get("clean_rows_out", 0)
        print(
            f"Dataset prepared and cached in {prep_duration:.2f} s "
            f"({raw_rows_in:,} raw rows -> {clean_rows_out:,} clean rows in view '{prepared.clean_view}')\n"
        )

        # 7. Execute Queries
        if args.explain:
            item = ANALYSES_BY_KEY[args.explain]
            fn = item["function"]
            print("=" * 80)
            print(f"EXPLAIN PLAN: {item['key']} ({item['title']})")
            print("=" * 80)
            result: QueryResult = fn(spark, filters=filters, view=prepared.clean_view)
            print("\nSQL Statement:")
            print(result.sql)
            print("\nSpark Physical Plan:")
            result.df.explain(True)
            print("\n" + "=" * 80)
            return 0

        analyses_to_run = [ANALYSES_BY_KEY[args.only]] if args.only else ANALYSES
        total_queries = len(analyses_to_run)
        total_query_start = time.perf_counter()

        for idx, item in enumerate(analyses_to_run, start=1):
            key = item["key"]
            category = item["category"]
            title = item["title"]
            fn = item["function"]

            print("=" * 80)
            print(f"[{idx}/{total_queries}] [{category.upper()}] {title} (key: {key})")
            print("=" * 80)

            t0 = time.perf_counter()
            result = fn(spark, filters=filters, view=prepared.clean_view)
            row_count = result.df.count()
            elapsed = time.perf_counter() - t0

            print("\nSQL:")
            print(result.sql.strip())
            print(f"\nPreview (first 10 rows of {row_count:,}):")
            result.df.show(10, truncate=False)
            print(f"Rows returned: {row_count:,} | Execution time: {elapsed:.3f} s\n")

        total_elapsed = time.perf_counter() - total_query_start
        print("=" * 80)
        print(f"ALL QUERIES COMPLETED: {total_queries} analysis executed in {total_elapsed:.2f} s")
        print("=" * 80)

        # 8. Save report if requested
        if args.save_report:
            report_path = PROJECT_ROOT / "docs" / "analysis_results.txt"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            content = dual_out.get_content()
            sanitized = sanitize_report_text(content)
            report_path.write_text(sanitized, encoding="utf-8")
            print(f"\n[INFO] Analysis report saved to: {display_path(report_path)}")

        return 0

    finally:
        sys.stdout = dual_out.original


if __name__ == "__main__":
    sys.exit(main())

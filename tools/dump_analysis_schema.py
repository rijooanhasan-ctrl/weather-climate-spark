"""
scripts/dump_analysis_schema.py
Dumps the exact schema (columns and data types) of all 22 analysis queries.
Saves to docs/analysis_schema.txt.
"""

import inspect
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import PRIMARY_DATASET_PATH
from src.cleaner import clean
from src.data_loader import load_dataset
from src.queries.registry import ANALYSES
from src.spark_session import get_spark_session


def main():
    spark = get_spark_session("AnalysisSchemaDumper")
    load_res = load_dataset(spark, PRIMARY_DATASET_PATH)
    clean(spark, std_view=load_res.standardized_view, view_name="weather_clean")

    output_lines = []
    output_lines.append("=" * 80)
    output_lines.append("WEATHER & CLIMATE SPARK SQL ANALYSIS ENGINE - FULL SCHEMA DUMP (22 ANALYSES)")
    output_lines.append("=" * 80)
    output_lines.append("")

    for item in ANALYSES:
        key = item["key"]
        title = item["title"]
        fn = item["function"]

        sig = inspect.signature(fn)
        params_info = []
        for p_name, p in sig.parameters.items():
            if p_name in ("spark", "filters", "view", "kwargs", "params"):
                continue
            default_val = p.default if p.default is not inspect.Parameter.empty else None
            params_info.append(f"{p_name}={default_val}")

        params_str = ", ".join(params_info) if params_info else "none"

        # Execute query with defaults
        res = fn(spark, view="weather_clean")
        fields = [(f.name, f.dataType.simpleString()) for f in res.df.schema.fields]

        output_lines.append(f"KEY: {key}")
        output_lines.append(f"TITLE: {title}")
        output_lines.append(f"PARAMS: {params_str}")
        output_lines.append(f"COLUMNS COUNT: {len(fields)}")
        output_lines.append("SCHEMA:")
        for col_name, col_type in fields:
            output_lines.append(f"  - {col_name}: {col_type}")
        output_lines.append("-" * 60)

    full_text = "\n".join(output_lines)
    print(full_text)

    schema_file = PROJECT_ROOT / "docs" / "analysis_schema.txt"
    with open(schema_file, "w", encoding="utf-8") as f:
        f.write(full_text + "\n")
    print(f"\n[INFO] Saved full schema dump to: docs/analysis_schema.txt")


if __name__ == "__main__":
    main()


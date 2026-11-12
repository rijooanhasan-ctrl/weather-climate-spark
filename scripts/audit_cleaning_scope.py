"""
scripts/audit_cleaning_scope.py
Audit script for Part A3: Cleaning fill scope.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings
from src.cleaner import clean
from src.data_loader import load_dataset
from src.spark_session import get_spark_session


def main():
    print("=" * 80)
    print("PART A3(a): settings.FILL_COLUMNS")
    print("=" * 80)
    print(f"settings.FILL_COLUMNS = {settings.FILL_COLUMNS}")
    print(f"settings.MAX_FILL_GAP_DAYS = {settings.MAX_FILL_GAP_DAYS}")
    print()

    print("=" * 80)
    print("PART A3(a): SOURCE OF STEP 5 IN src/cleaner.py")
    print("=" * 80)
    with open("src/cleaner.py", "r", encoding="utf-8") as f:
        lines = f.readlines()
    step5_code = "".join(lines[274:404])
    print(step5_code)
    print()

    print("=" * 80)
    print("PART A3(a): REAL DATA RUN - report.steps[4]['sql']")
    print("=" * 80)
    spark = get_spark_session("CleaningScopeAudit")
    load_res = load_dataset(spark, settings.PRIMARY_DATASET_PATH)
    df_clean, report = clean(spark, std_view=load_res.standardized_view, view_name="clean_audit_a3")
    print(f"report.steps[4]['description']: {report.steps[4]['description']}")
    print()
    print("report.steps[4]['sql']:")
    print(report.steps[4]["sql"])


if __name__ == "__main__":
    main()

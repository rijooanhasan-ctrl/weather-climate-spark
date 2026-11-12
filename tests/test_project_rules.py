"""
tests/test_project_rules.py
===========================
Automated governance tests enforcing architectural rules:
1. Pure Spark SQL only: no RDD operations, no createDataFrame from Python/Pandas,
   no python UDFs, no pandas_udf in src/.
2. Queries isolation: no module under src/queries/ imports pandas.
3. Row limit safety: QueryResult.to_pandas() raises when exceeding ANALYSIS_RESULT_MAX_ROWS.
"""

from __future__ import annotations

import ast
from pathlib import Path
import pytest

import config.settings as settings
from config.settings import ANALYSIS_RESULT_MAX_ROWS
from src.queries.base import QueryResult
from src.spark_session import get_spark_session


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
QUERIES_DIR = SRC_DIR / "queries"


def test_no_forbidden_spark_constructs_in_src():
    """
    Ensure no code in src/ uses RDDs, parallelize, createDataFrame, udf, or pandas_udf.
    """
    forbidden_tokens = [
        ".rdd",
        "parallelize",
        "createDataFrame",
        "udf(",
        "pandas_udf",
    ]

    violations = []
    for py_file in SRC_DIR.rglob("*.py"):
        content = py_file.read_text(encoding="utf-8")
        for token in forbidden_tokens:
            if token in content:
                # Exclude comments or docstrings by checking lines
                lines = content.splitlines()
                for line_no, line in enumerate(lines, start=1):
                    stripped = line.strip()
                    if token in stripped and not stripped.startswith("#"):
                        violations.append(
                            f"{py_file.relative_to(PROJECT_ROOT)}:{line_no}: found '{token}' in '{stripped}'"
                        )

    assert not violations, f"Forbidden Spark constructs detected:\n" + "\n".join(violations)


def test_no_pandas_in_queries():
    """
    Ensure no module in src/queries/ imports pandas (queries must remain pure Spark SQL).
    """
    violations = []
    for py_file in QUERIES_DIR.glob("*.py"):
        content = py_file.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "pandas" or alias.name.startswith("pandas."):
                        violations.append(f"{py_file.relative_to(PROJECT_ROOT)} imports {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module == "pandas" or (node.module and node.module.startswith("pandas.")):
                    violations.append(f"{py_file.relative_to(PROJECT_ROOT)} imports from {node.module}")

    assert not violations, f"pandas imports detected in src/queries/:\n" + "\n".join(violations)


def test_query_result_to_pandas_row_limit(monkeypatch):
    """
    QueryResult.to_pandas() must raise ValueError if row count exceeds ANALYSIS_RESULT_MAX_ROWS.
    """
    spark = get_spark_session("TestRowLimit")
    df = spark.sql("SELECT id FROM range(10)")

    res = QueryResult(
        key="test_limit",
        title="Test Limit",
        description="Testing row limit enforcement",
        sql="SELECT id FROM range(10)",
        df=df,
    )

    # Monkeypatch settings limit to 5 (lower than 10 rows in df)
    monkeypatch.setattr(settings, "ANALYSIS_RESULT_MAX_ROWS", 5)

    with pytest.raises(ValueError, match="exceeding safe limit"):
        res.to_pandas()

    # Reset limit to 20 (higher than 10 rows in df)
    monkeypatch.setattr(settings, "ANALYSIS_RESULT_MAX_ROWS", 20)
    pdf = res.to_pandas()
    assert len(pdf) == 10


def test_no_forbidden_analytics_in_charts_and_insights():
    """
    charts.py and insights.py must NOT perform analytical recomputation using
    .groupby(, .corr(, .rolling(, .resample(, polyfit, or linregress.
    All analytical metrics originate strictly from Spark SQL.
    """
    forbidden_tokens = [
        ".groupby(",
        ".corr(",
        ".rolling(",
        ".resample(",
        "polyfit",
        "linregress",
    ]

    target_files = [
        SRC_DIR / "dashboard" / "charts.py",
        SRC_DIR / "dashboard" / "insights.py",
    ]

    violations = []
    for py_file in target_files:
        assert py_file.is_file(), f"Expected file {py_file} does not exist"
        content = py_file.read_text(encoding="utf-8")
        lines = content.splitlines()
        for line_no, line in enumerate(lines, start=1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            for tok in forbidden_tokens:
                if tok in stripped:
                    violations.append(
                        f"{py_file.name}:{line_no}: forbidden analytics call '{tok}' in '{stripped}'"
                    )

    assert not violations, (
        f"Forbidden analytics recomputations found in visualization/insight modules:\n"
        + "\n".join(violations)
    )


def test_streamlit_import_boundaries():
    """
    Streamlit must ONLY be imported by app.py and modules in src/dashboard/ui/.
    All other modules in src/ (including src/dashboard/*.py outside ui/) must remain
    pure Python/Spark without Streamlit dependencies.
    """
    violations = []

    # 1. Check all python files under src/ (except src/dashboard/ui/)
    for py_file in SRC_DIR.rglob("*.py"):
        # Allow files in src/dashboard/ui
        if "src/dashboard/ui" in py_file.as_posix():
            continue
        content = py_file.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "streamlit" or alias.name.startswith("streamlit."):
                        violations.append(f"{py_file.relative_to(PROJECT_ROOT)} imports {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module == "streamlit" or (node.module and node.module.startswith("streamlit.")):
                    violations.append(f"{py_file.relative_to(PROJECT_ROOT)} imports from {node.module}")

    assert not violations, f"Illegal streamlit imports detected outside UI layer:\n" + "\n".join(violations)


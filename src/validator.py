"""
src/validator.py
================
Spark SQL validation logic and data quality reporting for ingested weather datasets.

Inspects raw and standardized Spark views to verify column mapping integrity,
row counts, date parsing, numeric casting, null rates, sentinel values,
physical plausibility ranges, duplicate primary keys, and location coverage.
All checks are executed through Spark SQL without Python UDFs or RDD operations.
"""

from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType

from config.schema import (
    PLAUSIBLE_RANGES,
    REQUIRED_COLUMNS,
    SENTINEL_VALUES,
    STANDARD_SCHEMA,
    UNITS,
)
from config.settings import (
    DATE_PARSE_ERROR_BELOW,
    MIN_ROWS,
    NULL_WARN_ABOVE,
    NUMERIC_PARSE_WARN_BELOW,
    OUT_OF_RANGE_UNIT_WARN_ABOVE,
)


@dataclass
class ValidationReport:
    """Structured report produced by the dataset validator."""
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    info: dict[str, Any] = field(default_factory=dict)
    column_stats: list[dict[str, Any]] = field(default_factory=list)
    sql_log: list[dict[str, str]] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """True if and only if there are zero validation errors."""
        return len(self.errors) == 0

    def to_dict(self) -> dict[str, Any]:
        """Convert the report to a standard Python dictionary."""
        data = asdict(self)
        data["is_valid"] = self.is_valid
        return data


def validate(
    spark: SparkSession,
    raw_view: str,
    std_view: str | None,
    mapping: dict[str, str | None],
    default_location: str | None = None,
    original_row_count: int | None = None,
) -> ValidationReport:
    """
    Validate raw and standardized weather datasets using Spark SQL.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    raw_view : str
        Temporary view name of the raw un-typed dataset.
    std_view : str | None
        Temporary view name of the standardized typed dataset, or None if
        standardization could not be performed.
    mapping : dict[str, str | None]
        Mapping from standard column name to original CSV header name.
    default_location : str | None, optional
        Fallback location name if 'location' column is missing from CSV.
    original_row_count : int | None, optional
        Precomputed row count if known.

    Returns
    -------
    ValidationReport
        Comprehensive validation findings including errors, warnings,
        column stats, and execution SQL query logs.
    """
    report = ValidationReport()

    def _execute_logged_sql(title: str, sql: str):
        report.sql_log.append({"title": title, "sql": sql.strip()})
        return spark.sql(sql)

    report.info["raw_view"] = raw_view
    report.info["std_view"] = std_view
    report.info["numeric_parse_failures"] = {}
    report.info["null_counts"] = {}
    report.info["sentinel_counts"] = {}
    report.info["out_of_range_counts"] = {}
    report.info["duplicate_keys_count"] = 0

    # ── 1. Check Required Columns and Duplicate Targets ─────────────────────────
    for col in REQUIRED_COLUMNS:
        if col == "location" and default_location is not None:
            continue
        if mapping.get(col) is None:
            report.errors.append(f"Missing required column: '{col}' is not mapped.")

    mapped_originals = [orig for orig in mapping.values() if orig is not None]
    orig_counts = Counter(mapped_originals)
    for orig, count in orig_counts.items():
        if count > 1:
            targets = [std_col for std_col, o in mapping.items() if o == orig]
            report.errors.append(
                f"Original column '{orig}' is mapped to multiple standard columns: {', '.join(targets)}."
            )

    # ── 2. Total Rows Check ───────────────────────────────────────────────────
    target_view = std_view if std_view is not None else raw_view
    row_count_sql = f"SELECT COUNT(*) AS total FROM {target_view}"
    report.sql_log.append({"title": "Count total dataset rows", "sql": row_count_sql.strip()})
    if original_row_count is not None:
        total_rows = original_row_count
    else:
        total_row = spark.sql(row_count_sql).collect()[0]
        total_rows = int(total_row["total"])
    report.info["total_rows"] = total_rows

    if total_rows == 0:
        report.errors.append("Dataset has 0 rows.")
        return report

    if total_rows < MIN_ROWS:
        report.warnings.append(
            f"Dataset has only {total_rows} row(s) (minimum recommended: {MIN_ROWS})."
        )

    # If standardized view is missing (e.g. unmapped date), stop further column checks
    # If standardized view is missing (e.g. unmapped date), stop further column checks
    if std_view is None:
        return report

    # ── Merged Standardized View Profiling Query (Phase 9 A3 / Phase 8 B2) ─────
    # Consolidates date parsing, numeric parsing, null counts, sentinels,
    # physical plausibility ranges, and summary statistics into a single query.
    numeric_columns = [
        f.name for f in STANDARD_SCHEMA.fields if isinstance(f.dataType, DoubleType)
    ]
    mapped_numeric = [col for col in numeric_columns if mapping.get(col) is not None]
    columns_to_inspect = [
        f.name for f in STANDARD_SCHEMA.fields
        if mapping.get(f.name) is not None or f.name in REQUIRED_COLUMNS
    ]
    sentinel_str = ", ".join(str(s) for s in SENTINEL_VALUES)
    inspected_ranges = [col for col in mapped_numeric if col in PLAUSIBLE_RANGES]

    merged_select_items = [
        "COUNT(date) AS valid_dates",
        "CAST(MIN(date) AS STRING) AS min_date",
        "CAST(MAX(date) AS STRING) AS max_date",
    ]

    for col in mapped_numeric:
        merged_select_items.append(f"COUNT(`{col}`) AS `{col}_parsed_cnt`")
        merged_select_items.append(
            f"COUNT(CASE WHEN `{col}` IN ({sentinel_str}) THEN 1 END) AS `{col}_sentinels`"
        )
        merged_select_items.append(
            f"MIN(CASE WHEN `{col}` NOT IN ({sentinel_str}) THEN `{col}` END) AS `{col}_min`"
        )
        merged_select_items.append(
            f"AVG(CASE WHEN `{col}` NOT IN ({sentinel_str}) THEN `{col}` END) AS `{col}_avg`"
        )
        merged_select_items.append(
            f"MAX(CASE WHEN `{col}` NOT IN ({sentinel_str}) THEN `{col}` END) AS `{col}_max`"
        )
        merged_select_items.append(
            f"COUNT(CASE WHEN `{col}` NOT IN ({sentinel_str}) THEN 1 END) AS `{col}_cnt`"
        )

    for col in columns_to_inspect:
        merged_select_items.append(f"COUNT(*) - COUNT(`{col}`) AS `{col}_nulls`")

    for col in inspected_ranges:
        low, high = PLAUSIBLE_RANGES[col]
        merged_select_items.append(
            f"COUNT(CASE WHEN (`{col}` < {low} OR `{col}` > {high}) AND `{col}` NOT IN ({sentinel_str}) THEN 1 END) AS `{col}_out`"
        )
        merged_select_items.append(
            f"COUNT(CASE WHEN `{col}` IS NOT NULL AND `{col}` NOT IN ({sentinel_str}) THEN 1 END) AS `{col}_valid`"
        )

    # Raw numeric query (executes on raw_view)
    raw_counts = None
    if mapped_numeric:
        raw_select_items = []
        for col in mapped_numeric:
            orig = mapping[col]
            escaped_orig = orig.replace("`", "``")
            raw_select_items.append(
                f"COUNT(CASE WHEN trim(`{escaped_orig}`) != '' AND `{escaped_orig}` IS NOT NULL THEN 1 END) AS `{col}_raw_cnt`"
            )
        raw_num_sql = f"SELECT {', '.join(raw_select_items)} FROM {raw_view}"
        raw_counts = _execute_logged_sql("Count non-empty raw numeric cells", raw_num_sql).collect()[0]

    # Execute single merged standardized view query
    merged_sql = f"SELECT {', '.join(merged_select_items)} FROM {std_view}"
    merged_row = spark.sql(merged_sql).collect()[0]

    # ── 3. Date Parse Check ───────────────────────────────────────────────────
    valid_dates: int = int(merged_row["valid_dates"])
    report.info["min_date"] = merged_row["min_date"]
    report.info["max_date"] = merged_row["max_date"]
    date_parse_rate = valid_dates / total_rows if total_rows > 0 else 0.0
    report.info["date_parse_rate"] = date_parse_rate

    report.sql_log.append({
        "title": "Verify date parsing and span",
        "sql": f"SELECT COUNT(*) AS total, COUNT(date) AS valid_dates, CAST(MIN(date) AS STRING) AS min_date, CAST(MAX(date) AS STRING) AS max_date FROM {std_view}".strip(),
    })

    if date_parse_rate < DATE_PARSE_ERROR_BELOW:
        report.errors.append(
            f"Date parse rate ({date_parse_rate:.1%}) is below error threshold {DATE_PARSE_ERROR_BELOW:.0%}."
        )
    elif valid_dates < total_rows:
        invalid_count = total_rows - valid_dates
        report.warnings.append(
            f"{invalid_count} row(s) have invalid or unparseable dates ({invalid_count / total_rows:.1%})."
        )

    # ── 4. Numeric Parse Check Per Mapped Column ──────────────────────────────
    if mapped_numeric and raw_counts is not None:
        report.sql_log.append({
            "title": "Count successfully parsed numeric values",
            "sql": f"SELECT {', '.join([f'COUNT(`{col}`) AS `{col}_parsed_cnt`' for col in mapped_numeric])} FROM {std_view}".strip(),
        })
        for col in mapped_numeric:
            raw_cnt = int(raw_counts[f"{col}_raw_cnt"])
            parsed_cnt = int(merged_row[f"{col}_parsed_cnt"])
            if raw_cnt > 0 and parsed_cnt < raw_cnt:
                failed_cnt = raw_cnt - parsed_cnt
                report.info["numeric_parse_failures"][col] = failed_cnt
                parse_pct = parsed_cnt / raw_cnt
                if parse_pct < NUMERIC_PARSE_WARN_BELOW:
                    report.warnings.append(
                        f"Column '{col}' had {failed_cnt} value(s) that failed numeric parsing "
                        f"({parse_pct:.1%} parsed, below threshold {NUMERIC_PARSE_WARN_BELOW:.0%})."
                    )
                else:
                    report.warnings.append(
                        f"Column '{col}' had {failed_cnt} value(s) that failed numeric parsing ({parse_pct:.1%} parsed)."
                    )

    # ── 5. Null Value Check ───────────────────────────────────────────────────
    report.sql_log.append({
        "title": "Check null value proportions",
        "sql": f"SELECT {', '.join([f'COUNT(*) - COUNT(`{col}`) AS `{col}_nulls`' for col in columns_to_inspect])} FROM {std_view}".strip(),
    })
    for col in columns_to_inspect:
        null_cnt = int(merged_row[f"{col}_nulls"])
        report.info["null_counts"][col] = null_cnt
        null_rate = null_cnt / total_rows if total_rows > 0 else 0.0
        if null_rate > NULL_WARN_ABOVE:
            report.warnings.append(
                f"Column '{col}' has {null_cnt} null value(s) ({null_rate:.1%}, exceeds warning threshold {NULL_WARN_ABOVE:.0%})."
            )
        elif null_cnt > 0:
            report.warnings.append(
                f"Column '{col}' has {null_cnt} null value(s) ({null_rate:.1%})."
            )

    # ── 6. Sentinel Value Check ───────────────────────────────────────────────
    if mapped_numeric:
        report.sql_log.append({
            "title": "Check sentinel value presence",
            "sql": f"SELECT {', '.join([f'COUNT(CASE WHEN `{col}` IN ({sentinel_str}) THEN 1 END) AS `{col}_sentinels`' for col in mapped_numeric])} FROM {std_view}".strip(),
        })
        for col in mapped_numeric:
            s_cnt = int(merged_row[f"{col}_sentinels"])
            if s_cnt > 0:
                report.info["sentinel_counts"][col] = s_cnt
                report.warnings.append(
                    f"Column '{col}' contains {s_cnt} sentinel value(s) ({sentinel_str})."
                )

    # ── 7. Physical Plausibility Range Check ──────────────────────────────────
    if inspected_ranges:
        report.sql_log.append({
            "title": "Check physical plausibility ranges",
            "sql": f"SELECT {', '.join([f'COUNT(CASE WHEN (`{col}` < {PLAUSIBLE_RANGES[col][0]} OR `{col}` > {PLAUSIBLE_RANGES[col][1]}) AND `{col}` NOT IN ({sentinel_str}) THEN 1 END) AS `{col}_out`' for col in inspected_ranges])} FROM {std_view}".strip(),
        })
        for col in inspected_ranges:
            out_cnt = int(merged_row[f"{col}_out"])
            valid_cnt = int(merged_row[f"{col}_valid"])
            low, high = PLAUSIBLE_RANGES[col]
            if out_cnt > 0:
                report.info["out_of_range_counts"][col] = out_cnt
                out_pct = out_cnt / valid_cnt if valid_cnt > 0 else 0.0
                if out_pct > OUT_OF_RANGE_UNIT_WARN_ABOVE:
                    msg = (
                        f"Unit mismatch: column '{col}' has {out_cnt} value(s) ({out_pct:.1%}) outside plausible range "
                        f"[{low}, {high}]. Expected unit: {UNITS.get(col, '')}."
                    )
                    # Unit mismatch on required numeric column triggers validation error (C3 rule)
                    if col in REQUIRED_COLUMNS:
                        report.errors.append(msg)
                    else:
                        report.warnings.append(msg)
                else:
                    report.warnings.append(
                        f"Column '{col}' has {out_cnt} value(s) outside plausible range [{low}, {high}]."
                    )

    # ── 8. Duplicate Key Check ────────────────────────────────────────────────
    dup_sql = f"""
    SELECT COUNT(*) AS dup_count
    FROM (
        SELECT date, location
        FROM {std_view}
        WHERE date IS NOT NULL AND location IS NOT NULL
        GROUP BY date, location
        HAVING COUNT(*) > 1
    )
    """
    dup_row = _execute_logged_sql("Check duplicate (date, location) primary keys", dup_sql).collect()[0]
    dup_count = int(dup_row["dup_count"])
    report.info["duplicate_keys_count"] = dup_count
    if dup_count > 0:
        report.warnings.append(
            f"Found {dup_count} duplicate (date, location) key(s)."
        )

    # ── 9. Coverage Per Location ──────────────────────────────────────────────
    loc_sql = f"""
    SELECT
        location,
        COUNT(*) AS row_count,
        CAST(MIN(date) AS STRING) AS min_date,
        CAST(MAX(date) AS STRING) AS max_date,
        DATEDIFF(MAX(date), MIN(date)) + 1 AS span_days
    FROM {std_view}
    WHERE location IS NOT NULL
    GROUP BY location
    ORDER BY row_count DESC
    LIMIT 20
    """
    loc_rows = _execute_logged_sql("Profile location coverage and completeness", loc_sql).collect()
    report.info["locations"] = [
        {
            "location": str(r["location"]),
            "row_count": int(r["row_count"]),
            "min_date": r["min_date"],
            "max_date": r["max_date"],
            "span_days": int(r["span_days"]) if r["span_days"] is not None else None,
            "missing_days": max(0, int(r["span_days"]) - int(r["row_count"])) if r["span_days"] is not None else 0,
        }
        for r in loc_rows
    ]

    # ── 10. Column Statistics for Numeric Variables ───────────────────────────
    stats_select_items = []
    for col in mapped_numeric:
        stats_select_items.append(
            f"MIN(CASE WHEN `{col}` NOT IN ({sentinel_str}) THEN `{col}` END) AS `{col}_min`"
        )
        stats_select_items.append(
            f"AVG(CASE WHEN `{col}` NOT IN ({sentinel_str}) THEN `{col}` END) AS `{col}_avg`"
        )
        stats_select_items.append(
            f"MAX(CASE WHEN `{col}` NOT IN ({sentinel_str}) THEN `{col}` END) AS `{col}_max`"
        )
        stats_select_items.append(
            f"COUNT(CASE WHEN `{col}` NOT IN ({sentinel_str}) THEN 1 END) AS `{col}_cnt`"
        )

    if stats_select_items:
        stats_sql = f"SELECT {', '.join(stats_select_items)} FROM {std_view}"
        report.sql_log.append({
            "title": "Compute summary statistics for numeric variables",
            "sql": stats_sql.strip(),
        })
        for col in mapped_numeric:
            cnt = int(merged_row[f"{col}_cnt"])
            if cnt > 0:
                report.column_stats.append({
                    "column": col,
                    "unit": UNITS.get(col, ""),
                    "min": round(float(merged_row[f"{col}_min"]), 2) if merged_row[f"{col}_min"] is not None else None,
                    "avg": round(float(merged_row[f"{col}_avg"]), 2) if merged_row[f"{col}_avg"] is not None else None,
                    "max": round(float(merged_row[f"{col}_max"]), 2) if merged_row[f"{col}_max"] is not None else None,
                    "count": cnt,
                })

    return report

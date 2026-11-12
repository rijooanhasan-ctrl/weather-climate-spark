"""
src/cleaner.py
==============
Data cleaning pipeline for standardized weather datasets using Spark SQL.

Performs deterministic multi-step data cleansing:
1. Drops unusable rows (missing date or location).
2. Converts sentinel codes (-999, -9999, 9999) to NULL.
3. Sets out-of-range physical outliers to NULL (or drops rows on option).
4. Deduplicates (date, location) primary keys retaining the most complete record.
5. Fills short gaps (<= MAX_FILL_GAP_DAYS) via window-based linear interpolation.
   (Rainfall, latitude, and longitude are never interpolated).
6. Derives calendar and meteorological quality flags (year, month, day_of_year, season).
7. Produces final registered clean view with audit tracking and reporting.

All transformations execute via Spark SQL without Python UDFs or RDD operations.
"""

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType

from config.schema import PLAUSIBLE_RANGES, SENTINEL_VALUES, STANDARD_SCHEMA
from config.settings import (
    CLEAN_VIEW_NAME,
    FILL_COLUMNS,
    LOCATION_MAX_NULL_RATE,
    MAX_FILL_GAP_DAYS,
    STANDARDIZED_VIEW_NAME,
)


@dataclass
class CleaningReport:
    """Detailed audit report produced by the data cleaning pipeline."""
    steps: list[dict[str, Any]] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert cleaning report to dictionary."""
        return asdict(self)

    def to_json(self, path: Path | str) -> None:
        """Serialize cleaning report to formatted JSON on disk."""
        target_path = Path(path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with open(target_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, default=str)


def clean(
    spark: SparkSession,
    std_view: str = STANDARDIZED_VIEW_NAME,
    view_name: str = CLEAN_VIEW_NAME,
    options: dict[str, Any] | None = None,
) -> tuple[DataFrame, CleaningReport]:
    """
    Execute deterministic cleaning pipeline on standardized Spark SQL view.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    std_view : str, optional
        Source standardized view name.
    view_name : str, optional
        Target cleaned view name to register.
    options : dict, optional
        Cleaning options:
        - fill_gaps (bool, default True): Whether to interpolate short gaps.
        - drop_out_of_range_rows (bool, default False): Whether to drop rows
          with out-of-range temperature instead of setting to NULL.

    Returns
    -------
    tuple[DataFrame, CleaningReport]
        (Cleaned DataFrame, comprehensive CleaningReport).
    """
    opts = options or {}
    fill_gaps = opts.get("fill_gaps", True)
    drop_out_of_range = opts.get("drop_out_of_range_rows", False)

    report = CleaningReport()
    numeric_columns = [
        f.name for f in STANDARD_SCHEMA.fields if isinstance(f.dataType, DoubleType)
    ]
    standard_columns = [f.name for f in STANDARD_SCHEMA.fields]

    # Initial row count
    initial_rows = int(spark.sql(f"SELECT COUNT(*) AS n FROM {std_view}").collect()[0]["n"])
    current_rows = initial_rows

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 1: DROP UNUSABLE ROWS (missing date or blank/null location)
    # ─────────────────────────────────────────────────────────────────────────
    step1_sql = f"""
    SELECT *
    FROM {std_view}
    WHERE `date` IS NOT NULL
      AND `location` IS NOT NULL
      AND trim(`location`) != ''
    """
    df_step1 = spark.sql(step1_sql)
    df_step1.createOrReplaceTempView("clean_step_1")

    rows_after_1 = int(spark.sql("SELECT COUNT(*) AS n FROM clean_step_1").collect()[0]["n"])
    removed_1 = current_rows - rows_after_1

    report.steps.append({
        "step": 1,
        "description": "Drop unusable rows (null date, null location, or blank location)",
        "rows_before": current_rows,
        "rows_after": rows_after_1,
        "rows_removed": removed_1,
        "values_changed": {},
        "sql": step1_sql.strip(),
    })
    current_rows = rows_after_1

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 2: SENTINELS TO NULL
    # ─────────────────────────────────────────────────────────────────────────
    sentinel_str = ", ".join(str(s) for s in SENTINEL_VALUES)

    # Count sentinel values changed per column
    sentinel_cnt_clauses = [
        f"COUNT(CASE WHEN `{col}` IN ({sentinel_str}) THEN 1 END) AS `{col}`"
        for col in numeric_columns
    ]
    count_sentinels_sql = f"SELECT {', '.join(sentinel_cnt_clauses)} FROM clean_step_1"
    sentinel_counts_row = spark.sql(count_sentinels_sql).collect()[0]
    sentinel_changes = {
        col: int(sentinel_counts_row[col])
        for col in numeric_columns
        if int(sentinel_counts_row[col]) > 0
    }

    step2_select = ["`date`", "`location`"]
    for col in numeric_columns:
        step2_select.append(
            f"CASE WHEN `{col}` IN ({sentinel_str}) THEN NULL ELSE `{col}` END AS `{col}`"
        )

    step2_sql = f"""
    SELECT
        {",\n        ".join(step2_select)}
    FROM clean_step_1
    """
    df_step2 = spark.sql(step2_sql)
    df_step2.createOrReplaceTempView("clean_step_2")

    report.steps.append({
        "step": 2,
        "description": f"Convert sentinel codes ({sentinel_str}) to NULL in numeric columns",
        "rows_before": current_rows,
        "rows_after": current_rows,
        "rows_removed": 0,
        "values_changed": sentinel_changes,
        "sql": step2_sql.strip(),
    })

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3: OUT-OF-RANGE TO NULL (or drop temperature outliers)
    # ─────────────────────────────────────────────────────────────────────────
    range_cnt_clauses = []
    for col, (low, high) in PLAUSIBLE_RANGES.items():
        range_cnt_clauses.append(
            f"COUNT(CASE WHEN `{col}` IS NOT NULL AND (`{col}` < {low} OR `{col}` > {high}) THEN 1 END) AS `{col}`"
        )
    count_ranges_sql = f"SELECT {', '.join(range_cnt_clauses)} FROM clean_step_2"
    range_counts_row = spark.sql(count_ranges_sql).collect()[0]
    range_changes = {
        col: int(range_counts_row[col])
        for col in PLAUSIBLE_RANGES
        if int(range_counts_row[col]) > 0
    }

    step3_select = ["`date`", "`location`"]
    for col in numeric_columns:
        if col in PLAUSIBLE_RANGES:
            low, high = PLAUSIBLE_RANGES[col]
            step3_select.append(
                f"CASE WHEN `{col}` < {low} OR `{col}` > {high} THEN NULL ELSE `{col}` END AS `{col}`"
            )
        else:
            step3_select.append(f"`{col}`")

    if drop_out_of_range and "temperature" in PLAUSIBLE_RANGES:
        t_low, t_high = PLAUSIBLE_RANGES["temperature"]
        step3_sql = f"""
        SELECT
            {",\n            ".join(step3_select)}
        FROM clean_step_2
        WHERE `temperature` IS NULL OR (`temperature` >= {t_low} AND `temperature` <= {t_high})
        """
    else:
        step3_sql = f"""
        SELECT
            {",\n            ".join(step3_select)}
        FROM clean_step_2
        """

    df_step3 = spark.sql(step3_sql)
    df_step3.createOrReplaceTempView("clean_step_3")

    rows_after_3 = int(spark.sql("SELECT COUNT(*) AS n FROM clean_step_3").collect()[0]["n"])
    removed_3 = current_rows - rows_after_3

    report.steps.append({
        "step": 3,
        "description": "Convert physical range outliers to NULL (or drop rows on option)",
        "rows_before": current_rows,
        "rows_after": rows_after_3,
        "rows_removed": removed_3,
        "values_changed": range_changes,
        "sql": step3_sql.strip(),
    })
    current_rows = rows_after_3

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4: REMOVE DUPLICATES on (date, location)
    # ─────────────────────────────────────────────────────────────────────────
    dup_stats_sql = """
    SELECT
        COUNT(*) AS total_dup_keys,
        COALESCE(SUM(cnt - 1), 0) AS surplus_rows
    FROM (
        SELECT date, location, COUNT(*) AS cnt
        FROM clean_step_3
        GROUP BY date, location
        HAVING COUNT(*) > 1
    )
    """
    dup_stats_row = spark.sql(dup_stats_sql).collect()[0]
    total_dup_keys = int(dup_stats_row["total_dup_keys"])
    surplus_rows = int(dup_stats_row["surplus_rows"])

    completeness_expr = " + ".join(
        [f"(CASE WHEN `{col}` IS NOT NULL THEN 1 ELSE 0 END)" for col in numeric_columns]
    )

    step4_sql = f"""
    SELECT {', '.join([f'`{col}`' for col in standard_columns])}
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY date, location
                ORDER BY ({completeness_expr}) DESC, rainfall DESC NULLS LAST
            ) AS _rn
        FROM clean_step_3
    )
    WHERE _rn = 1
    """
    df_step4 = spark.sql(step4_sql)
    df_step4.createOrReplaceTempView("clean_step_4")

    rows_after_4 = int(spark.sql("SELECT COUNT(*) AS n FROM clean_step_4").collect()[0]["n"])

    report.steps.append({
        "step": 4,
        "description": "Remove duplicate (date, location) keys retaining most complete record",
        "rows_before": current_rows,
        "rows_after": rows_after_4,
        "rows_removed": surplus_rows,
        "values_changed": {"duplicate_keys": total_dup_keys, "surplus_rows_dropped": surplus_rows},
        "sql": step4_sql.strip(),
    })
    current_rows = rows_after_4

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 5: FILL SHORT GAPS (<= MAX_FILL_GAP_DAYS)
    # ─────────────────────────────────────────────────────────────────────────
    filled_changes: dict[str, int] = {}
    if fill_gaps:
        # Check which columns in FILL_COLUMNS actually have nulls in clean_step_4
        null_check_clauses = [
            f"COUNT(CASE WHEN `{col}` IS NULL THEN 1 END) AS `{col}`"
            for col in FILL_COLUMNS
        ]
        cnt_row = spark.sql(f"SELECT {', '.join(null_check_clauses)} FROM clean_step_4").collect()[0]
        cols_with_nulls = [col for col in FILL_COLUMNS if int(cnt_row[col]) > 0]

        if not cols_with_nulls:
            # Fast path: No missing values in any fillable column.
            # Bypass all window CTEs completely.
            no_interp_select = [f"`{col}`" for col in standard_columns]
            for col in FILL_COLUMNS:
                no_interp_select.append(f"CAST(false AS BOOLEAN) AS `{col}_was_filled`")

            step5_sql = f"""
            SELECT
                {",\n                ".join(no_interp_select)}
            FROM clean_step_4
            """
            df_step5 = spark.sql(step5_sql)
            df_step5.createOrReplaceTempView("clean_step_5")
        else:
            # Build window CTE ONLY for columns that actually have missing values
            w_select = []
            for col in standard_columns:
                if col in cols_with_nulls:
                    w_select.append(f"`{col}`")
                    w_select.append(
                        f"last(`{col}`, true) OVER (PARTITION BY location ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS `_pv_{col}`"
                    )
                    w_select.append(
                        f"last(CASE WHEN `{col}` IS NOT NULL THEN date END, true) OVER (PARTITION BY location ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS `_pd_{col}`"
                    )
                    w_select.append(
                        f"first(`{col}`, true) OVER (PARTITION BY location ORDER BY date ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS `_nv_{col}`"
                    )
                    w_select.append(
                        f"first(CASE WHEN `{col}` IS NOT NULL THEN date END, true) OVER (PARTITION BY location ORDER BY date ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS `_nd_{col}`"
                    )
                else:
                    w_select.append(f"`{col}`")

            interp_select = []
            for col in standard_columns:
                if col in cols_with_nulls:
                    interp_select.append(f"""
                    CASE
                        WHEN `{col}` IS NULL
                            AND `_pv_{col}` IS NOT NULL
                            AND `_nv_{col}` IS NOT NULL
                            AND (datediff(`_nd_{col}`, `_pd_{col}`) - 1) <= {MAX_FILL_GAP_DAYS}
                            AND datediff(`_nd_{col}`, `_pd_{col}`) > 1
                        THEN ROUND(`_pv_{col}` + (CAST(datediff(date, `_pd_{col}`) AS DOUBLE) / datediff(`_nd_{col}`, `_pd_{col}`)) * (`_nv_{col}` - `_pv_{col}`), 4)
                        ELSE `{col}`
                    END AS `{col}`""")
                else:
                    interp_select.append(f"`{col}`")

            # Add was_filled flag columns for each column in FILL_COLUMNS
            for col in FILL_COLUMNS:
                if col in cols_with_nulls:
                    interp_select.append(f"""
                    CASE
                        WHEN `{col}` IS NULL
                            AND `_pv_{col}` IS NOT NULL
                            AND `_nv_{col}` IS NOT NULL
                            AND (datediff(`_nd_{col}`, `_pd_{col}`) - 1) <= {MAX_FILL_GAP_DAYS}
                            AND datediff(`_nd_{col}`, `_pd_{col}`) > 1
                        THEN true
                        ELSE false
                    END AS `{col}_was_filled`""")
                else:
                    interp_select.append(f"CAST(false AS BOOLEAN) AS `{col}_was_filled`")

            step5_sql = f"""
            WITH windowed AS (
                SELECT
                    {",\n                    ".join(w_select)}
                FROM clean_step_4
            )
            SELECT
                {",\n                ".join(interp_select)}
            FROM windowed
            """
            df_step5 = spark.sql(step5_sql)
            df_step5.createOrReplaceTempView("clean_step_5")

            # Count how many values were filled per column
            filled_cnt_clauses = [
                f"COUNT(CASE WHEN `{col}_was_filled` THEN 1 END) AS `{col}`"
                for col in cols_with_nulls
            ]
            cnt_filled_sql = f"SELECT {', '.join(filled_cnt_clauses)} FROM clean_step_5"
            cnt_filled_row = spark.sql(cnt_filled_sql).collect()[0]
            for col in cols_with_nulls:
                cnt = int(cnt_filled_row[col])
                if cnt > 0:
                    filled_changes[col] = cnt
    else:
        # Interpolation disabled: add false boolean flags
        no_interp_select = [f"`{col}`" for col in standard_columns]
        for col in FILL_COLUMNS:
            no_interp_select.append(f"CAST(false AS BOOLEAN) AS `{col}_was_filled`")

        step5_sql = f"""
        SELECT
            {",\n            ".join(no_interp_select)}
        FROM clean_step_4
        """
        df_step5 = spark.sql(step5_sql)
        df_step5.createOrReplaceTempView("clean_step_5")

    report.steps.append({
        "step": 5,
        "description": f"Linearly interpolate gaps <= {MAX_FILL_GAP_DAYS} days per location (rainfall never filled)",
        "rows_before": current_rows,
        "rows_after": current_rows,
        "rows_removed": 0,
        "values_changed": filled_changes,
        "sql": step5_sql.strip(),
    })

    # ─────────────────────────────────────────────────────────────────────────
    # STEPS 6 & 7: QUALITY FLAGS & FINAL CLEAN VIEW
    # ─────────────────────────────────────────────────────────────────────────
    flag_columns = [f"`{col}_was_filled`" for col in FILL_COLUMNS]

    step6_sql = f"""
    SELECT
        {", ".join([f"`{col}`" for col in standard_columns])},
        year(date) AS year,
        month(date) AS month,
        dayofyear(date) AS day_of_year,
        CASE
            WHEN month(date) IN (1, 2) THEN 'Winter'
            WHEN month(date) IN (3, 4, 5) THEN 'Summer'
            WHEN month(date) IN (6, 7, 8, 9) THEN 'Monsoon'
            WHEN month(date) IN (10, 11, 12) THEN 'Post-monsoon'
        END AS season,
        {", ".join(flag_columns)}
    FROM clean_step_5
    ORDER BY location, date
    """
    df_clean = spark.sql(step6_sql)
    df_clean.cache()
    df_clean.createOrReplaceTempView(view_name)

    clean_rows = int(spark.sql(f"SELECT COUNT(*) AS n FROM {view_name}").collect()[0]["n"])

    report.steps.append({
        "step": 6,
        "description": "Derive temporal quality flags (year, month, day_of_year, season) and register final view",
        "rows_before": current_rows,
        "rows_after": clean_rows,
        "rows_removed": 0,
        "values_changed": {},
        "sql": step6_sql.strip(),
    })

    # ─────────────────────────────────────────────────────────────────────────
    # COMPOSE SUMMARY & WARNINGS
    # ─────────────────────────────────────────────────────────────────────────
    # 1. Remaining nulls per standard column
    null_clauses = [
        f"COUNT(CASE WHEN `{col}` IS NULL THEN 1 END) AS `{col}`"
        for col in standard_columns
    ]
    rem_null_sql = f"SELECT {', '.join(null_clauses)} FROM {view_name}"
    rem_null_row = spark.sql(rem_null_sql).collect()[0]
    remaining_nulls = {col: int(rem_null_row[col]) for col in standard_columns}

    # 2. Location row counts and temperature completeness
    loc_stats_sql = f"""
    SELECT
        location,
        COUNT(*) AS total_rows,
        COUNT(temperature) AS valid_temp
    FROM {view_name}
    GROUP BY location
    ORDER BY location
    """
    loc_rows = spark.sql(loc_stats_sql).collect()
    location_row_counts: dict[str, int] = {}
    low_quality_locations: list[str] = []

    for r in loc_rows:
        loc = str(r["location"])
        tot = int(r["total_rows"])
        valid_t = int(r["valid_temp"])
        location_row_counts[loc] = tot
        temp_null_rate = (tot - valid_t) / tot if tot > 0 else 0.0
        if temp_null_rate > LOCATION_MAX_NULL_RATE:
            low_quality_locations.append(loc)
            report.warnings.append(
                f"Location '{loc}' flagged as low quality: temperature null rate is "
                f"{temp_null_rate:.1%} (exceeds threshold {LOCATION_MAX_NULL_RATE:.0%})."
            )

    # 3. Warning if interpolation was applied
    total_interpolated = sum(filled_changes.values())
    if total_interpolated > 0:
        details = ", ".join(f"{col}: {cnt}" for col, cnt in filled_changes.items())
        report.warnings.append(
            f"Interpolation applied to fill {total_interpolated} missing value(s) ({details}). "
            f"These are estimated values, not direct sensor measurements."
        )

    report.summary = {
        "raw_rows_in": initial_rows,
        "clean_rows_out": clean_rows,
        "rows_removed": {
            "unusable_rows": removed_1,
            "out_of_range_dropped": removed_3,
            "duplicate_surplus_dropped": surplus_rows,
            "total_rows_removed": initial_rows - clean_rows,
        },
        "values_changed": {
            "sentinels_to_null": sentinel_changes,
            "out_of_range_to_null": range_changes,
            "interpolated": filled_changes,
        },
        "remaining_nulls": remaining_nulls,
        "location_row_counts": location_row_counts,
        "low_quality_locations": low_quality_locations,
    }

    return df_clean, report

"""
src/data_loader.py
==================
CSV ingestion, header parsing, date format detection, schema standardization,
and end-to-end dataset loading with Spark SQL.

All data parsing and type transformations occur through Spark SQL without
Python UDFs or RDD operations.
"""

from collections import Counter
import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from config.schema import (
    REQUIRED_COLUMNS,
    STANDARD_SCHEMA,
    suggest_column_mapping,
)
from config.settings import (
    DATE_FORMAT_CANDIDATES,
    RAW_VIEW_NAME,
    STANDARDIZED_VIEW_NAME,
    SUPPORTED_DELIMITERS,
)
from src.sql_utils import quote_identifier, sql_string_literal
from src.utils import display_path, to_spark_path
from src.validator import ValidationReport, validate


class DatasetLoadError(Exception):
    """Raised when dataset loading encounters fatal structural or format errors."""
    pass


@dataclass
class LoadResult:
    """End-to-end result of loading, standardizing, and validating a dataset."""
    path_display: str
    delimiter: str
    original_columns: list[str]
    mapping: dict[str, str | None]
    date_format_info: dict[str, Any]
    standardized_view: str | None
    validation: ValidationReport
    sql_log: list[dict[str, str]] = field(default_factory=list)


def read_csv_header(path: Path | str) -> tuple[list[str], str]:
    """
    Read the first non-empty line of a CSV file, auto-detect the delimiter,
    and validate column header integrity.

    Parameters
    ----------
    path : Path | str
        Path to the CSV file on disk.

    Returns
    -------
    tuple[list[str], str]
        (list of cleaned column names, detected delimiter string).

    Raises
    ------
    DatasetLoadError
        If the file is empty, missing a header, contains empty column names,
        or contains duplicate column names.
    """
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        raise DatasetLoadError(f"Dataset file does not exist: {display_path(file_path)}")

    first_line: str = ""
    with open(file_path, mode="r", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                first_line = stripped
                break

    if not first_line:
        raise DatasetLoadError(f"The CSV file has no header row: {display_path(file_path)}")

    # Detect delimiter by candidate occurrence count
    counts = {d: first_line.count(d) for d in SUPPORTED_DELIMITERS}
    max_count = max(counts.values()) if counts else 0
    if max_count == 0:
        detected_delim = ","
    else:
        # Tie-breaker preserves candidate order in SUPPORTED_DELIMITERS
        detected_delim = next(d for d in SUPPORTED_DELIMITERS if counts[d] == max_count)

    reader = csv.reader([first_line], delimiter=detected_delim)
    raw_columns = next(reader, [])
    columns = [c.strip().strip('"').strip("'") for c in raw_columns]

    if not columns:
        raise DatasetLoadError("The CSV file has no header row.")

    # Check for empty column names
    empty_indices = [i for i, c in enumerate(columns) if not c]
    if empty_indices:
        raise DatasetLoadError(
            f"CSV header contains empty column name(s) at index {empty_indices}."
        )

    # Check for duplicate column names
    col_counts = Counter(columns)
    duplicates = [c for c, count in col_counts.items() if count > 1]
    if duplicates:
        raise DatasetLoadError(
            f"Duplicate or empty column names in CSV header: {', '.join(duplicates)}"
        )

    return columns, detected_delim


def load_raw_csv(
    spark: SparkSession,
    path: Path | str,
    delimiter: str,
    view_name: str = RAW_VIEW_NAME,
) -> DataFrame:
    """
    Load raw CSV into Spark with all columns as un-typed StringType.

    Using inferSchema=False prevents Spark from failing on unexpected strings,
    allowing Spark SQL to inspect and report validation issues cleanly.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    path : Path | str
        Path to CSV file.
    delimiter : str
        Delimiter character.
    view_name : str, optional
        Temporary view name to register.

    Returns
    -------
    DataFrame
        Raw untyped DataFrame.
    """
    spark_path = to_spark_path(path)
    df = (
        spark.read
        .option("header", "true")
        .option("sep", delimiter)
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .csv(spark_path)
    )
    df.createOrReplaceTempView(view_name)
    return df


def detect_date_format(
    spark: SparkSession,
    raw_view: str,
    date_column: str,
) -> dict[str, Any]:
    """
    Detect the date format of a raw date column using a single Spark SQL query.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    raw_view : str
        Temporary view name of the raw un-typed dataset.
    date_column : str
        Name of the column containing date strings.

    Returns
    -------
    dict[str, Any]
        Dictionary with keys:
        - format: best matching format string
        - parse_rate: fraction of rows parsed successfully [0.0 - 1.0]
        - candidates: dict mapping format to parse rate
        - is_ambiguous: bool indicating multiple tied formats
        - note: explanatory message

    Raises
    ------
    DatasetLoadError
        If 0 rows can be parsed with any supported format.
    """
    escaped_col = date_column.replace("`", "``")
    fmt_clauses = [
        f"COUNT(to_date(`{escaped_col}`, '{fmt}')) AS `fmt_{i}`"
        for i, fmt in enumerate(DATE_FORMAT_CANDIDATES)
    ]

    sql = f"""
    SELECT
        COUNT(*) AS total_rows,
        {", ".join(fmt_clauses)}
    FROM {raw_view}
    WHERE `{escaped_col}` IS NOT NULL AND trim(`{escaped_col}`) != ''
    """

    row = spark.sql(sql).collect()[0]
    total_non_empty = int(row["total_rows"])

    if total_non_empty == 0:
        raise DatasetLoadError(
            f"No non-empty date values found in column '{date_column}'."
        )

    candidates: dict[str, float] = {}
    counts: dict[str, int] = {}
    for i, fmt in enumerate(DATE_FORMAT_CANDIDATES):
        cnt = int(row[f"fmt_{i}"])
        counts[fmt] = cnt
        candidates[fmt] = cnt / total_non_empty

    max_count = max(counts.values())
    if max_count == 0:
        raise DatasetLoadError(
            f"No dates could be parsed from column '{date_column}' with any supported format."
        )

    best_rate = max_count / total_non_empty
    top_formats = [fmt for fmt in DATE_FORMAT_CANDIDATES if counts[fmt] == max_count]
    best_format = top_formats[0]
    is_ambiguous = len(top_formats) > 1

    if is_ambiguous:
        note = (
            f"Multiple date formats parsed equal row counts ({', '.join(top_formats)}). "
            f"Prioritized format '{best_format}'."
        )
    else:
        note = f"Format '{best_format}' detected unambiguously ({best_rate:.1%} parsed)."

    return {
        "format": best_format,
        "parse_rate": best_rate,
        "candidates": candidates,
        "is_ambiguous": is_ambiguous,
        "note": note,
        "sql": sql.strip(),
    }


def standardize(
    spark: SparkSession,
    raw_view: str,
    mapping: dict[str, str | None],
    date_format: str,
    default_location: str | None = None,
    view_name: str = STANDARDIZED_VIEW_NAME,
) -> DataFrame:
    """
    Project and cast columns from raw_view into the exact STANDARD_SCHEMA structure
    using Spark SQL.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    raw_view : str
        Temporary view of the raw untyped data.
    mapping : dict[str, str | None]
        Mapping from standard schema column names to raw column names.
    date_format : str
        Date format string to use with to_date().
    default_location : str | None, optional
        Constant fallback value if 'location' is not mapped.
    view_name : str, optional
        Name of the standardized view to register.

    Returns
    -------
    DataFrame
        Standardized DataFrame.
    """
    select_clauses: list[str] = []

    # 1. date
    raw_date = mapping.get("date")
    if raw_date:
        escaped_date = quote_identifier(raw_date)
        select_clauses.append(f"to_date({escaped_date}, '{date_format}') AS `date`")
    else:
        select_clauses.append("CAST(NULL AS DATE) AS `date`")

    # 2. location
    raw_loc = mapping.get("location")
    if raw_loc:
        escaped_loc = quote_identifier(raw_loc)
        select_clauses.append(f"CAST({escaped_loc} AS STRING) AS `location`")
    elif default_location is not None:
        safe_loc = sql_string_literal(default_location)
        select_clauses.append(f"{safe_loc} AS `location`")
    else:
        select_clauses.append("CAST(NULL AS STRING) AS `location`")

    # 3. Numeric & coordinate columns in canonical STANDARD_SCHEMA order
    other_fields = [
        "latitude", "longitude", "temperature", "temp_max",
        "temp_min", "rainfall", "humidity", "wind_speed", "pressure"
    ]

    for field_name in other_fields:
        raw_col = mapping.get(field_name)
        if raw_col:
            escaped_col = quote_identifier(raw_col)
            select_clauses.append(f"try_cast({escaped_col} AS DOUBLE) AS `{field_name}`")
        else:
            select_clauses.append(f"CAST(NULL AS DOUBLE) AS `{field_name}`")

    sql = f"""
    SELECT
        {",\n        ".join(select_clauses)}
    FROM {raw_view}
    """

    df = spark.sql(sql)
    df.createOrReplaceTempView(view_name)
    return df


def load_dataset(
    spark: SparkSession,
    path: Path | str,
    mapping: dict[str, str | None] | None = None,
    date_format: str | None = None,
    default_location: str | None = None,
) -> LoadResult:
    """
    End-to-end pipeline: inspect CSV header, suggest column mapping, load raw CSV,
    detect date format, standardize schema, and validate dataset with Spark SQL.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    path : Path | str
        Path to CSV file.
    mapping : dict[str, str | None] | None, optional
        User-provided mapping overrides.
    date_format : str | None, optional
        User-provided date format override.
    default_location : str | None, optional
        Default location name to apply if CSV has no location column.

    Returns
    -------
    LoadResult
        Complete summary of ingestion, date detection, standardization,
        and validation.
    """
    sql_log: list[dict[str, str]] = []
    file_path = Path(path)

    # Step 1: Read header and detect delimiter
    orig_cols, delim = read_csv_header(file_path)

    # Step 2: Establish column mapping
    suggested = suggest_column_mapping(orig_cols)
    final_mapping: dict[str, str | None] = suggested.copy()
    if mapping is not None:
        final_mapping.update(mapping)

    # Step 3: Load raw untyped view
    raw_view = RAW_VIEW_NAME
    load_raw_csv(spark, file_path, delimiter=delim, view_name=raw_view)

    # Check if raw dataset has rows
    raw_count_sql = f"SELECT COUNT(*) AS total FROM {raw_view}"
    total_raw = int(spark.sql(raw_count_sql).collect()[0]["total"])
    sql_log.append({"title": "Check raw dataset row count", "sql": raw_count_sql.strip()})

    date_col = final_mapping.get("date")

    # If raw dataset has 0 rows or missing date column, validate directly without standardization
    if total_raw == 0 or date_col is None:
        val_report = validate(
            spark=spark,
            raw_view=raw_view,
            std_view=None,
            mapping=final_mapping,
            default_location=default_location,
            original_row_count=total_raw,
        )
        sql_log.extend(val_report.sql_log)
        date_info = {
            "format": None,
            "parse_rate": 0.0,
            "candidates": {},
            "is_ambiguous": False,
            "note": "Date column not mapped or dataset is empty.",
        }
        return LoadResult(
            path_display=display_path(file_path),
            delimiter=delim,
            original_columns=orig_cols,
            mapping=final_mapping,
            date_format_info=date_info,
            standardized_view=None,
            validation=val_report,
            sql_log=sql_log,
        )

    # Step 4: Detect or verify date format
    if date_format is None:
        date_info = detect_date_format(spark, raw_view, date_col)
        detected_fmt = date_info["format"]
        sql_log.append({"title": "Detect date format candidates", "sql": date_info.get("sql", "")})
    else:
        detected_fmt = date_format
        date_info = {
            "format": detected_fmt,
            "parse_rate": 1.0,
            "candidates": {detected_fmt: 1.0},
            "is_ambiguous": False,
            "note": "User-specified date format.",
        }

    # Step 5: Standardize dataset
    std_view = STANDARDIZED_VIEW_NAME
    standardize(
        spark=spark,
        raw_view=raw_view,
        mapping=final_mapping,
        date_format=detected_fmt,
        default_location=default_location,
        view_name=std_view,
    )

    # Step 6: Validate standardized dataset
    val_report = validate(
        spark=spark,
        raw_view=raw_view,
        std_view=std_view,
        mapping=final_mapping,
        default_location=default_location,
        original_row_count=total_raw,
    )
    sql_log.extend(val_report.sql_log)

    return LoadResult(
        path_display=display_path(file_path),
        delimiter=delim,
        original_columns=orig_cols,
        mapping=final_mapping,
        date_format_info=date_info,
        standardized_view=std_view,
        validation=val_report,
        sql_log=sql_log,
    )


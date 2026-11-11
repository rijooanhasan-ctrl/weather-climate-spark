"""
src/queries/base.py
===================
Base primitives, filtering abstractions, and execution containers for Spark SQL
weather and climate analysis queries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from pyspark.sql import DataFrame, SparkSession

import config.settings as settings
from config.settings import ANALYSIS_RESULT_MAX_ROWS, CLEAN_VIEW_NAME
from src.sql_utils import sql_string_literal


@dataclass
class Filters:
    """
    Validated query filters to restrict observations by location and time.

    Attributes
    ----------
    locations : list[str] or None
        List of location names. None = all locations; [] = empty list (produces 0 rows).
    start_date : str or date or None
        Start date (inclusive). Strings must match ISO YYYY-MM-DD.
    end_date : str or date or None
        End date (inclusive). Strings must match ISO YYYY-MM-DD.
    years : list[int] or None
        List of integer calendar years.
    months : list[int] or None
        List of integer calendar months (1 to 12).
    seasons : list[str] or None
        List of meteorological seasons ('Winter', 'Summer', 'Monsoon', 'Post-monsoon').
    """

    locations: list[str] | None = None
    start_date: str | date | None = None
    end_date: str | date | None = None
    years: list[int] | None = None
    months: list[int] | None = None
    seasons: list[str] | None = None

    def __post_init__(self) -> None:
        # Validate start_date
        if isinstance(self.start_date, str):
            try:
                self.start_date = date.fromisoformat(self.start_date)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid start_date '{self.start_date}'. Expected format YYYY-MM-DD."
                ) from exc

        # Validate end_date
        if isinstance(self.end_date, str):
            try:
                self.end_date = date.fromisoformat(self.end_date)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid end_date '{self.end_date}'. Expected format YYYY-MM-DD."
                ) from exc

        # Validate years
        if self.years is not None:
            validated_years = []
            for y in self.years:
                try:
                    validated_years.append(int(y))
                except (ValueError, TypeError) as exc:
                    raise ValueError(f"Invalid year '{y}'. Must be an integer.") from exc
            self.years = validated_years

        # Validate months
        if self.months is not None:
            validated_months = []
            for m in self.months:
                try:
                    int_m = int(m)
                    if not (1 <= int_m <= 12):
                        raise ValueError
                    validated_months.append(int_m)
                except (ValueError, TypeError) as exc:
                    raise ValueError(f"Invalid month '{m}'. Must be an integer between 1 and 12.") from exc
            self.months = validated_months

        # Validate seasons
        if self.seasons is not None:
            self.seasons = [str(s) for s in self.seasons]


def build_where(filters: Filters | None) -> str:
    """
    Construct an injection-safe SQL WHERE clause from a Filters object.

    All user-controlled strings and literals are escaped via sql_string_literal
    or converted to validated integers. Never emits unescaped user text.

    Parameters
    ----------
    filters : Filters or None
        The active filters to convert into SQL.

    Returns
    -------
    str
        Safe SQL condition (e.g. "location IN ('Mumbai') AND year >= 2015").
        Returns "1 = 1" if no filters are active, or "1 = 0" if locations is empty list.
    """
    if filters is None:
        return "1 = 1"

    clauses: list[str] = []

    # 1. Locations
    if filters.locations is not None:
        if len(filters.locations) == 0:
            return "1 = 0"  # Explicitly empty location filter must return zero rows
        escaped_locs = [sql_string_literal(loc) for loc in filters.locations]
        clauses.append(f"location IN ({', '.join(escaped_locs)})")

    # 2. Date ranges
    if filters.start_date is not None:
        clauses.append(f"date >= {sql_string_literal(filters.start_date.isoformat())}")

    if filters.end_date is not None:
        clauses.append(f"date <= {sql_string_literal(filters.end_date.isoformat())}")

    # 3. Years
    if filters.years is not None and len(filters.years) > 0:
        clauses.append(f"year IN ({', '.join(str(y) for y in filters.years)})")

    # 4. Months
    if filters.months is not None and len(filters.months) > 0:
        clauses.append(f"month IN ({', '.join(str(m) for m in filters.months)})")

    # 5. Seasons
    if filters.seasons is not None and len(filters.seasons) > 0:
        escaped_seasons = [sql_string_literal(s) for s in filters.seasons]
        clauses.append(f"season IN ({', '.join(escaped_seasons)})")

    return " AND ".join(clauses) if clauses else "1 = 1"


@dataclass
class QueryResult:
    """
    Represents the output of an analytical Spark SQL query with lazy evaluation metadata.
    """

    key: str
    title: str
    description: str
    sql: str
    parameters: dict[str, Any] = field(default_factory=dict)
    df: DataFrame = None  # type: ignore

    def to_pandas(self, limit: int | None = None) -> Any:
        """
        Convert query results to a Pandas DataFrame, enforcing row-count safety limits.

        Parameters
        ----------
        limit : int, optional
            Maximum rows to retrieve.

        Returns
        -------
        pandas.DataFrame
            The collected query results.

        Raises
        ------
        ValueError
            If the total row count exceeds ANALYSIS_RESULT_MAX_ROWS.
        """
        # Read the current max limit dynamically from settings or module global
        max_rows = getattr(settings, "ANALYSIS_RESULT_MAX_ROWS", ANALYSIS_RESULT_MAX_ROWS)
        total_rows = self.df.count()

        if total_rows > max_rows:
            raise ValueError(
                f"Query result '{self.key}' produced {total_rows} rows, exceeding safe limit of {max_rows}. "
                "Please apply more selective filters (e.g. location or date range) before retrieving to Pandas."
            )

        if limit is not None:
            return self.df.limit(limit).toPandas()
        return self.df.toPandas()


def run_query(
    spark: SparkSession,
    key: str,
    title: str,
    description: str,
    sql: str,
    parameters: dict[str, Any] | None = None,
) -> QueryResult:
    """
    Execute a Spark SQL analytical statement and package into a QueryResult.
    """
    clean_sql = sql.strip()
    df = spark.sql(clean_sql)
    return QueryResult(
        key=key,
        title=title,
        description=description,
        sql=clean_sql,
        parameters=parameters or {},
        df=df,
    )

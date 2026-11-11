"""
src/pipeline.py
===============
Integrated data pipeline that loads, standardizes, validates, cleans, and caches
weather datasets for analytical querying and dashboard consumption.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from config.settings import CLEAN_VIEW_NAME, PRIMARY_DATASET_PATH
from src.cleaner import CleaningReport, clean
from src.data_loader import LoadResult, load_dataset


class PipelineError(Exception):
    """Raised when data pipeline validation or ingestion encounters fatal errors."""


@dataclass
class PreparedData:
    """Encapsulates the end-to-end prepared dataset and audit artifacts."""

    load_result: LoadResult
    cleaning_report: CleaningReport
    clean_view: str


def prepare(
    spark: SparkSession,
    path: Path | str = PRIMARY_DATASET_PATH,
    mapping: dict[str, str | None] | None = None,
    date_format: str | None = None,
    default_location: str | None = None,
    clean_options: dict[str, Any] | None = None,
    view_name: str = CLEAN_VIEW_NAME,
) -> PreparedData:
    """
    Load, standardize, validate, clean, and cache a weather dataset.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    path : Path or str, default PRIMARY_DATASET_PATH
        Path to the CSV dataset file.
    mapping : dict[str, str | None], optional
        Column mapping overrides.
    date_format : str, optional
        Date format override.
    default_location : str, optional
        Default location name if missing from source.
    clean_options : dict, optional
        Cleaning pipeline option overrides (e.g. fill_gaps, drop_out_of_range).
    view_name : str, default CLEAN_VIEW_NAME
        Name of the clean temporary view to register and cache.

    Returns
    -------
    PreparedData
        Contains load_result, cleaning_report, and clean_view.

    Raises
    ------
    PipelineError
        If dataset validation contains errors.
    """
    load_result = load_dataset(
        spark=spark,
        path=path,
        mapping=mapping,
        date_format=date_format,
        default_location=default_location,
    )

    if not load_result.validation.is_valid:
        error_lines = "\n  - ".join(load_result.validation.errors)
        raise PipelineError(
            f"Dataset validation failed with {len(load_result.validation.errors)} error(s):\n  - {error_lines}"
        )

    _, cleaning_report = clean(
        spark=spark,
        std_view=load_result.standardized_view,
        view_name=view_name,
        options=clean_options,
    )

    # Cache clean table in Spark catalog for fast interactive querying
    spark.catalog.cacheTable(view_name)

    return PreparedData(
        load_result=load_result,
        cleaning_report=cleaning_report,
        clean_view=view_name,
    )

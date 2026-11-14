"""
scripts/experiment_worker.py
============================
Worker script to run the 22 analysis queries under a specific Spark configuration
in an isolated fresh process and output timings and numeric checksums as JSON.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession

from config.settings import APP_NAME, CLEAN_VIEW_NAME, PROCESSED_DATA_DIR
from src.queries.base import Filters
from src.queries.registry import ANALYSES
from src.spark_session import find_java_home
from src.utils import to_spark_path


def load_cached_clean_view(spark: SparkSession) -> None:
    """Fast-load the verified clean dataset and cache eagerly."""
    csv_file = PROCESSED_DATA_DIR / "weather_clean.csv"
    spark_path = to_spark_path(csv_file)
    raw_clean = spark.read.option("header", "true").csv(spark_path)
    raw_clean.createOrReplaceTempView("_csv_clean")

    clean_sql = f"""
    SELECT
        try_cast(date AS DATE) AS date,
        CAST(location AS STRING) AS location,
        try_cast(latitude AS DOUBLE) AS latitude,
        try_cast(longitude AS DOUBLE) AS longitude,
        try_cast(temperature AS DOUBLE) AS temperature,
        try_cast(temp_max AS DOUBLE) AS temp_max,
        try_cast(temp_min AS DOUBLE) AS temp_min,
        try_cast(rainfall AS DOUBLE) AS rainfall,
        try_cast(humidity AS DOUBLE) AS humidity,
        try_cast(wind_speed AS DOUBLE) AS wind_speed,
        try_cast(pressure AS DOUBLE) AS pressure,
        try_cast(year AS INT) AS year,
        try_cast(month AS INT) AS month,
        try_cast(day_of_year AS INT) AS day_of_year,
        CAST(season AS STRING) AS season,
        try_cast(temperature_was_filled AS BOOLEAN) AS temperature_was_filled,
        try_cast(temp_max_was_filled AS BOOLEAN) AS temp_max_was_filled,
        try_cast(temp_min_was_filled AS BOOLEAN) AS temp_min_was_filled,
        try_cast(humidity_was_filled AS BOOLEAN) AS humidity_was_filled,
        try_cast(wind_speed_was_filled AS BOOLEAN) AS wind_speed_was_filled,
        try_cast(pressure_was_filled AS BOOLEAN) AS pressure_was_filled
    FROM _csv_clean
    """
    df = spark.sql(clean_sql)
    df.cache()
    df.createOrReplaceTempView(CLEAN_VIEW_NAME)
    df.count()


def run_worker(config: dict[str, str], master: str) -> None:
    # Auto-detect JAVA_HOME if needed
    import os
    if "JAVA_HOME" not in os.environ:
        jh = find_java_home()
        if jh:
            os.environ["JAVA_HOME"] = jh

    builder = SparkSession.builder.appName("ConfigExperimentWorker").master(master)
    # Required Windows loopback settings
    builder = (
        builder
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
    )

    for k, v in config.items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Fast-load clean data into view
    load_cached_clean_view(spark)

    filters = Filters()
    checksums: dict[str, dict[str, Any]] = {}

    t0 = time.perf_counter()
    for item in ANALYSES:
        key = item["key"]
        fn = item["function"]
        res = fn(spark, filters=filters, view=CLEAN_VIEW_NAME)
        pdf = res.to_pandas()

        sums: dict[str, float] = {}
        for col in pdf.columns:
            if pdf[col].dtype.kind in "iufc":
                try:
                    s = pdf[col].sum()
                    sums[col] = round(float(s), 2)
                except Exception:
                    pass

        checksums[key] = {
            "rows": len(pdf),
            "sums": sums,
        }
    t1 = time.perf_counter()
    total_time = t1 - t0

    spark.stop()

    output = {
        "duration_s": total_time,
        "checksums": checksums,
    }
    print("EXPERIMENT_OUTPUT_JSON:" + json.dumps(output))


def main() -> None:
    raw_json = sys.argv[1] if len(sys.argv) > 1 else "{}"
    spec = json.loads(raw_json)
    config = spec.get("config", {})
    master = spec.get("master", "local[*]")
    run_worker(config, master)


if __name__ == "__main__":
    main()

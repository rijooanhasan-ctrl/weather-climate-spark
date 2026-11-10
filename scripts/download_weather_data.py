"""
scripts/download_weather_data.py
================================
Downloads real, multi-city, multi-year daily weather observations from the
Open-Meteo Historical Weather API (ERA5 reanalysis).

Standard Library Only: urllib, json, csv, argparse, time, os, tempfile, datetime, pathlib.
"""

import argparse
import csv
import datetime
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# Project root on sys.path to access configuration
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (  # noqa: E402
    DATASET_END_DATE,
    DATASET_START_DATE,
    DEFAULT_CITIES,
    PRIMARY_DATASET_PATH,
)
from src.utils import display_path  # noqa: E402

ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"

# Canonical CSV Header Order
CANONICAL_HEADERS = [
    "date",
    "location",
    "latitude",
    "longitude",
    "temperature",
    "temp_max",
    "temp_min",
    "rainfall",
    "humidity",
    "wind_speed",
    "pressure",
]

DAILY_VARIABLES = (
    "temperature_2m_mean,"
    "temperature_2m_max,"
    "temperature_2m_min,"
    "precipitation_sum,"
    "relative_humidity_2m_mean,"
    "wind_speed_10m_mean,"
    "pressure_msl_mean"
)

HOURLY_VARIABLES = (
    "relative_humidity_2m,"
    "wind_speed_10m,"
    "pressure_msl"
)


def _format_cell(val: object) -> str:
    """Format a value for CSV output; nulls become empty strings."""
    if val is None or val == "":
        return ""
    return str(val)


def _make_request_with_retry(url: str, max_retries: int = 5) -> tuple[int, dict, str]:
    """
    Perform HTTP GET with exponential backoff for 429, 5xx, and network errors.

    Returns
    -------
    tuple[int, dict, str]
        (status_code, json_payload_or_empty_dict, error_reason_string)
    """
    backoff = 2
    for attempt in range(1, max_retries + 1):
        req = urllib.request.Request(url, headers={"User-Agent": "WeatherClimateSpark/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return 200, data, ""
        except urllib.error.HTTPError as exc:
            status = exc.code
            err_body = exc.read().decode("utf-8", errors="ignore")
            reason = ""
            try:
                err_json = json.loads(err_body)
                reason = err_json.get("reason", err_body)
            except Exception:
                reason = err_body

            # Do not retry 400 (Bad Request) unless it's handled by fallback
            if status == 400:
                return 400, {}, reason

            # Retry 429 (rate limit) or 5xx (server error)
            if status == 429 or status >= 500:
                if attempt == max_retries:
                    return status, {}, f"HTTP {status}: {reason}"
                time.sleep(backoff)
                backoff = min(60, backoff * 2)
                continue

            return status, {}, f"HTTP {status}: {reason}"

        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt == max_retries:
                return 0, {}, f"Network error: {exc}"
            time.sleep(backoff)
            backoff = min(60, backoff * 2)

    return 0, {}, "Exhausted retries"


def fetch_city_data(
    city: dict[str, str | float],
    start_date: str,
    end_date: str,
) -> tuple[str, list[list[str]]]:
    """
    Fetch historical daily weather for a city.

    Tries primary 'daily' endpoint first. If unsupported (HTTP 400),
    falls back to 'hourly-aggregated' daily mean calculation in plain Python.

    Returns
    -------
    tuple[str, list[list[str]]]
        (method_used, rows_for_city)
    """
    city_name = str(city["name"])
    lat = float(city["latitude"])
    lon = float(city["longitude"])

    # 1. Attempt Primary: Direct daily variables
    params_daily = {
        "latitude": f"{lat:.4f}",
        "longitude": f"{lon:.4f}",
        "start_date": start_date,
        "end_date": end_date,
        "daily": DAILY_VARIABLES,
        "timezone": "Asia/Kolkata",
    }
    url_daily = f"{ARCHIVE_API_URL}?{urllib.parse.urlencode(params_daily)}"
    status, payload, err = _make_request_with_retry(url_daily)

    method_used = "daily"

    # 2. Check for fallback requirement (HTTP 400 mentioning unsupported daily variable)
    if status == 400 and any(kw in err.lower() for kw in ["daily", "variable", "not supported", "unknown"]):
        # Fallback: Daily for temp & precip, Hourly for humidity, wind, pressure
        params_fallback = {
            "latitude": f"{lat:.4f}",
            "longitude": f"{lon:.4f}",
            "start_date": start_date,
            "end_date": end_date,
            "daily": "temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum",
            "hourly": HOURLY_VARIABLES,
            "timezone": "Asia/Kolkata",
        }
        url_fallback = f"{ARCHIVE_API_URL}?{urllib.parse.urlencode(params_fallback)}"
        status_fb, payload_fb, err_fb = _make_request_with_retry(url_fallback)
        if status_fb != 200:
            raise RuntimeError(f"Fallback request failed for {city_name}: {err_fb}")

        method_used = "hourly-aggregated"
        payload = payload_fb
    elif status != 200:
        raise RuntimeError(f"Failed to download data for {city_name}: {err}")

    # 3. Parse and Validate Payload
    daily = payload.get("daily", {})
    dates = daily.get("time", [])

    # Validate dates length matches exact expected calendar days
    d_start = datetime.date.fromisoformat(start_date)
    d_end = datetime.date.fromisoformat(end_date)
    expected_days = (d_end - d_start).days + 1

    if len(dates) != expected_days:
        raise ValueError(
            f"Validation failed for {city_name}: expected {expected_days} dates between "
            f"{start_date} and {end_date}, but API returned {len(dates)}."
        )

    t_mean = daily.get("temperature_2m_mean", [])
    t_max = daily.get("temperature_2m_max", [])
    t_min = daily.get("temperature_2m_min", [])
    precip = daily.get("precipitation_sum", [])

    if method_used == "daily":
        humidity = daily.get("relative_humidity_2m_mean", [])
        wind = daily.get("wind_speed_10m_mean", [])
        pressure = daily.get("pressure_msl_mean", [])
    else:
        # Compute daily mean from hourly variables
        hourly = payload.get("hourly", {})
        h_times = hourly.get("time", [])
        h_rh = hourly.get("relative_humidity_2m", [])
        h_wind = hourly.get("wind_speed_10m", [])
        h_pres = hourly.get("pressure_msl", [])

        # Accumulators per date
        daily_rh_acc: dict[str, list[float]] = {d: [] for d in dates}
        daily_wind_acc: dict[str, list[float]] = {d: [] for d in dates}
        daily_pres_acc: dict[str, list[float]] = {d: [] for d in dates}

        for ht, rh, w, p in zip(h_times, h_rh, h_wind, h_pres):
            d_key = ht[:10]
            if d_key in daily_rh_acc:
                if rh is not None:
                    daily_rh_acc[d_key].append(float(rh))
                if w is not None:
                    daily_wind_acc[d_key].append(float(w))
                if p is not None:
                    daily_pres_acc[d_key].append(float(p))

        humidity = [
            round(sum(daily_rh_acc[d]) / len(daily_rh_acc[d]), 2) if daily_rh_acc[d] else None
            for d in dates
        ]
        wind = [
            round(sum(daily_wind_acc[d]) / len(daily_wind_acc[d]), 2) if daily_wind_acc[d] else None
            for d in dates
        ]
        pressure = [
            round(sum(daily_pres_acc[d]) / len(daily_pres_acc[d]), 2) if daily_pres_acc[d] else None
            for d in dates
        ]

    # Validate value array lengths
    for var_name, var_arr in [
        ("temperature_2m_mean", t_mean),
        ("temperature_2m_max", t_max),
        ("temperature_2m_min", t_min),
        ("precipitation_sum", precip),
        ("relative_humidity", humidity),
        ("wind_speed", wind),
        ("pressure", pressure),
    ]:
        if len(var_arr) != len(dates):
            raise ValueError(
                f"Validation failed for {city_name}: array '{var_name}' has {len(var_arr)} items, "
                f"expected {len(dates)}."
            )

    rows: list[list[str]] = []
    for i, date_str in enumerate(dates):
        row = [
            date_str,
            city_name,
            f"{lat:.4f}",
            f"{lon:.4f}",
            _format_cell(t_mean[i]),
            _format_cell(t_max[i]),
            _format_cell(t_min[i]),
            _format_cell(precip[i]),
            _format_cell(humidity[i]),
            _format_cell(wind[i]),
            _format_cell(pressure[i]),
        ]
        rows.append(row)

    return method_used, rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download historical weather data from Open-Meteo Archive API."
    )
    parser.add_argument(
        "--output",
        default=str(PRIMARY_DATASET_PATH),
        help=f"Target output CSV path (default: {PRIMARY_DATASET_PATH})",
    )
    parser.add_argument(
        "--start",
        default=DATASET_START_DATE,
        help=f"Start date YYYY-MM-DD (default: {DATASET_START_DATE})",
    )
    parser.add_argument(
        "--end",
        default=DATASET_END_DATE,
        help=f"End date YYYY-MM-DD (default: {DATASET_END_DATE})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite output file if it already exists.",
    )
    args = parser.parse_args()

    out_path = Path(args.output).resolve()

    # If output file already exists and --force not given, exit 0
    if out_path.exists() and not args.force:
        print(f"Dataset already exists at {display_path(out_path)}. Use --force to overwrite. Exiting.")
        sys.exit(0)

    out_path.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("  Weather and Climate Pattern Discovery – Dataset Acquisition")
    print("  Data Source: Open-Meteo Historical Weather API (ERA5 Reanalysis)")
    print(f"  Date Range : {args.start} to {args.end}")
    print(f"  Cities ({len(DEFAULT_CITIES)})   : {', '.join(c['name'] for c in DEFAULT_CITIES)}")
    print(f"  Target File: {display_path(out_path)}")
    print("=" * 70)

    # Use a tempfile in the same folder for all-or-nothing atomic replacement
    temp_fd, temp_file_path_str = tempfile.mkstemp(
        dir=out_path.parent,
        prefix="temp_weather_download_",
        suffix=".csv",
    )
    os.close(temp_fd)
    temp_path = Path(temp_file_path_str)

    overall_method = "daily"
    rows_per_city: dict[str, int] = {}
    total_rows = 0

    try:
        with open(temp_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(CANONICAL_HEADERS)

            for idx, city in enumerate(DEFAULT_CITIES, 1):
                city_name = str(city["name"])
                print(f"[{idx}/{len(DEFAULT_CITIES)}] Fetching {city_name}...", end="", flush=True)

                method, city_rows = fetch_city_data(city, args.start, args.end)
                if method != "daily":
                    overall_method = method

                writer.writerows(city_rows)
                count = len(city_rows)
                rows_per_city[city_name] = count
                total_rows += count
                print(f" Done ({count:,} records, method: {method})")

                # Respect rate limits between cities
                if idx < len(DEFAULT_CITIES):
                    time.sleep(2)

        # Atomic replacement: only commits if all cities succeeded
        os.replace(temp_path, out_path)

    except Exception as exc:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass
        print(f"\n[ERROR] Dataset acquisition failed: {exc}", file=sys.stderr)
        sys.exit(1)

    file_size_mb = out_path.stat().st_size / (1024 * 1024)

    print("\n" + "=" * 70)
    print("  ACQUISITION COMPLETE")
    print(f"  Method Used   : {overall_method}")
    print(f"  Rows Per City : {', '.join(f'{k}: {v:,}' for k, v in rows_per_city.items())}")
    print(f"  Total Rows    : {total_rows:,}")
    print(f"  Date Range    : {args.start} to {args.end}")
    print(f"  File Size     : {file_size_mb:.2f} MB")
    print(f"  Destination   : {display_path(out_path)}")
    print("=" * 70)


if __name__ == "__main__":
    main()


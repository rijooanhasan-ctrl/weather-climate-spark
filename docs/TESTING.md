# Testing, Variants, and Scale Verification (Phase 9)

## 1. Dataset Variants and Edge Cases Benchmark (V1 – V10)

All 10 dataset variants were synthesized dynamically from real historical observations (`data/raw/india_weather_daily.csv`) and validated end-to-end through `scripts/run_variants_benchmark.py` and `tests/test_variants.py`.

| Variant | Input Description | Expected Behavior | Actual Result | Status |
| :--- | :--- | :--- | :--- | :---: |
| **V1** | Headers `Date;City;Temp;Rain;RH;Wind;Pres`, semicolon delimiter, UTF-8 BOM, dates in `dd/MM/yyyy`, 3 cities (2018–2022). | Auto-detects semicolon delimiter, strips BOM, auto-maps alternate aliases, parses dates, completes 22 queries. | Delimiter `;` detected, date format `dd/MM/yyyy` detected, 5,478 rows in/out, 22/22 queries succeeded. | **PASS** |
| **V2** | Missing `location` column (single city Bengaluru, all years). | Applies fallback default location (`--location-name "Bengaluru"`) during standardization, no nulls. | Single station Bengaluru assigned across all 5,844 rows, 0 null locations, 22/22 queries succeeded. | **PASS** |
| **V3** | Temperature in Kelvin (real values + 273.15, label: "unit test only"). | Flagged by unit-mismatch validation rule (>50% out-of-range), exits with code 1, aborts `prepare()` with `PipelineError`, no analysis queries run. | Validation error reported (500 values / 100.0% out of range), `prepare()` raises `PipelineError`, 0 queries run. | **PASS** |
| **V4** | One city, one year (365 rows: Bengaluru 2021). | Short time span (<5 years) handled gracefully by `temp_trend`, returns `insufficient data` without crashing. | 365 rows processed, trend query returns `insufficient data`, all 22 queries succeeded. | **PASS** |
| **V5** | Two cities, three years (Bengaluru & Mumbai 2020–2022). | Intermittent/short multi-city series handled gracefully, trend returns `insufficient data`, no failure. | 2,192 rows processed, trend query returns `insufficient data`, 22/22 queries succeeded. | **PASS** |
| **V6** | Rainfall column entirely empty for one city (Mumbai). | Rainfall remains NULL for Mumbai and is NEVER interpolated; rainfall analyses and insights handle missing data. | Mumbai rainfall has 0 non-null records, 0 values interpolated, 22/22 queries completed cleanly. | **PASS** |
| **V7** | 10% rows deleted deterministically, plus 3 gaps of 5+ days removed. | Gaps <= 3 days linearly interpolated with `was_filled` flags set; gaps >= 5 days left untouched; incomplete years flagged. | 640 rows processed, long gaps preserved, incomplete years flagged, 22/22 queries succeeded. | **PASS** |
| **V8** | Location names with mixed case and surrounding whitespace (`"mumbai"`, `" Mumbai "`, `"MUMBAI"`). | Spark SQL groups them as distinct entities; preserved as separate locations without silent merging (documented limitation). | 5 distinct location variants preserved in clean view (`[' MUMBAI ', ' Mumbai ', ' mumbai ', 'Mumbai', 'mumbai']`). | **PASS** |
| **V9** | Dates in `yyyyMMdd`, column names with surrounding spaces and quotes (`" Observation Date "`, `" City Name "`, etc.). | Auto-detects `yyyyMMdd` format, strips surrounding quotes and whitespace from column headers, maps correctly. | `yyyyMMdd` format detected, headers cleanly stripped and mapped, 400 rows, 22/22 queries succeeded. | **PASS** |
| **V10** | Duplicate column headers (`["date", "location", "temperature", ..., "temperature"]`). | Header parsing raises friendly `DatasetLoadError` with duplicate column names, exits with code 2. | Friendly `DatasetLoadError` raised: `Duplicate or empty column names in CSV header: temperature`. | **PASS** |

---

## 2. Unit-Mismatch Validation Rule (Phase 9 C3)

To prevent erroneous analyses on datasets with mismatched physical measurement units (e.g. Fahrenheit or Kelvin temperature, inches of precipitation, or meters/second wind speed):

* **Rule Specification**: If more than `OUT_OF_RANGE_UNIT_WARN_ABOVE` (50.0%) of non-null values in any **required numeric column** (`temperature`, `rainfall`, `humidity`, `wind_speed`, `pressure`) fall outside defined `PLAUSIBLE_RANGES`, the validator classifies this finding as a **critical validation error** (`report.errors.append(...)`), rather than a soft warning.
* **Pipeline Enforcement**: When `load_result.validation.errors` contains any entries, `src/pipeline.py prepare()` immediately aborts execution by raising `PipelineError`, preventing invalid downstream cleaning, aggregation, or dashboard caching.

---

## 3. Scale Test Results (~935k Rows, 20x Replication)

A distributed scaling benchmark was executed via `scripts/scale_test.py` on 935,040 rows (20x replication of real historical observations, 64.29 MB on disk):

* **Replication**: 20 replica stations per location (`Bengaluru_1` through `Bengaluru_20`, etc.).
* **Pipeline Preparation**: `prepare()` completed in **33.73 s** producing 935,040 clean rows.
* **Correctness Check**: Verified that for 3 replicated stations (`Bengaluru_1`, `Mumbai_5`, `Delhi_10`), yearly average temperature and total rainfall across all 48 station-years match original historical metrics with **0.00% difference**.
* **Total 22 Analyses Duration**: **11.84 s** across all 22 Spark SQL queries on local single-node JVM.
* **Top 3 Slowest Analyses**:
  1. `extreme_thresholds`: 2.830 s (calculates multi-decadal percentile approximations across 160 station groups)
  2. `temp_yearly`: 1.490 s (multi-year aggregation with anomaly reference windows)
  3. `climate_year_comparison`: 1.457 s (cross-station comparative joins)
* **Distributed Architecture Note**: On a production multi-node Spark cluster, the identical Spark SQL queries execute across distributed worker nodes without code changes, partitioning the ~935k rows across executor cores with stage shuffles parallelized horizontally.

---

## 4. Known System Limitations

1. **Case and Whitespace Sensitivity in Location Names (V8)**: Location identifiers are treated strictly as distinct categorical entities in Spark SQL group-by queries (`"Mumbai"` != `"mumbai"` != `" Mumbai "`). Upstream dataset providers should normalize casing prior to ingestion if station identity merging is desired.
2. **Single Active In-Memory Dataset**: The system maintains one active dataset in memory at a time (`weather_raw`, `weather_std`, `weather_clean`). Uploading or selecting a new dataset evicts previously cached views.
3. **Historical Data Span (16 Years)**: Built-in dataset spans 2010 to 2025 (ERA5 reanalysis). Trend regressions require at least 5 complete calendar years; time spans below 5 years report `insufficient data`.
4. **No Automatic Unit Conversion**: Datasets with non-standard units (e.g. Kelvin, Fahrenheit, inches) are caught and rejected by the validator to preserve audit integrity; automatic unit conversion is not performed in the engine.

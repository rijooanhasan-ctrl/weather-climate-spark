# Weather and Climate Pattern Discovery using Apache Spark SQL

[![Python 3.13](https://img.shields.io/badge/Python-3.13.3-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.3-orange.svg)](https://spark.apache.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.40.2-red.svg)](https://streamlit.io/)
[![Plotly](https://img.shields.io/badge/Plotly-5.24.1-green.svg)](https://plotly.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

An enterprise-grade, big-data analytics platform engineered to discover historical weather, monsoon dynamics, and decadal climate patterns from observational meteorological datasets.

The system uses **Apache Spark SQL** as the primary computation engine, operating purely on distributed relational query plans without Python UDFs or RDD serialization overhead. An interactive **Streamlit + Plotly** dashboard presents real-time multi-dimensional climatological analytics, automated natural language insights, and interactive data quality audits.

---

## ⚡ Quick Start

Get running in three simple commands:

```bash
# 1. Install dependencies inside your virtual environment
pip install -r requirements.txt

# 2. Run the automated test suite
pytest -v

# 3. Launch the interactive dashboard
streamlit run app.py
```

---

## 🏛️ System Architecture

The analytical engine is structured into a deterministic 5-layer pipeline. Every stage from raw CSV ingestion to multi-decadal trend regression is executed purely in Apache Spark SQL:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DATA SOURCE LAYER                                │
│          data/raw/india_weather_daily.csv  (46,752 historical records)      │
│          User Uploads (CSV up to 200 MB, auto-delimiter sniffing)           │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. STANDARDIZATION LAYER (Pure Spark SQL)                                   │
│    • Schema standardization via try_cast() & to_date()                      │
│    • Whitespace normalization: TRIM(CAST(location AS STRING))               │
│    • Canonical 11-column projection registered as temporary in-memory view  │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 2. VALIDATION LAYER (Unified Single-Pass Spark SQL Aggregation)              │
│    • Null proportions, date continuity, sentinel code detection             │
│    • Physical plausible range violation profiling                           │
│    • Duplicate primary key detection (date, location)                       │
│    • Rejection of invalid units (e.g. Kelvin temperatures > 50% anomaly)   │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 3. CLEANING PIPELINE (Deterministic Spark SQL Windowing)                    │
│    • Unusable record pruning (null date / location)                         │
│    • Sentinel replacement to SQL NULL (-999, -9999, 9999)                   │
│    • Deduplication via completeness ranking: ROW_NUMBER() OVER (...)        │
│    • Linear gap filling (gaps <= 3 days per station; rainfall never filled)  │
│    • Temporal flag derivation: year, month, day_of_year, season             │
│    • Materialization & caching of "weather_clean" view                      │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 4. ANALYSIS ENGINE (22 Analytical Spark SQL Models)                         │
│    • Temperature: anomalies, climatology, trends (regr_slope), rankings     │
│    • Rainfall: monsoon seasonality, Z-score anomalies, IMD heavy rain       │
│    • Climate: 95th/5th percentile extremes, heatwave spells, correlations   │
│    • KPIs: headline extremes, records, coverage summary                     │
│    • In-memory thread-safe ResultCache with single-flight deduplication     │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 5. PRESENTATION LAYER (Streamlit & Plotly)                                  │
│    • 8 Section Views: Overview, Temperature, Rainfall, Climate, Quality, ...│
│    • 10 Automated Natural Language Insights derived directly from Spark SQL │
│    • Interactive Plotly visualizations (dark theme, responsive zoom)        │
│    • Form-based filter batching & lazy per-analysis rendering               │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Analytical Query Catalog (22 Models)

All 22 analytical queries run strictly in Spark SQL:

| Domain | Query Key | Description |
| :--- | :--- | :--- |
| **Temperature** | `temp_yearly` | Annual mean, max, min temperatures and baseline anomalies per station |
| | `temp_monthly_climatology` | 12-month climatological baseline cycle across stations |
| | `temp_monthly_series` | Full chronological monthly temperature time series |
| | `temp_location_ranking` | Thermal ranking of locations with all-time historical extremes and dates |
| | `temp_trend` | Multi-decadal warming rate via least-squares linear regression (`regr_slope`) |
| | `temp_seasonal` | Temperature distribution partitioned across meteorological seasons |
| **Rainfall** | `rain_monthly` | Monthly rainfall accumulation and station heatmap matrices |
| | `rain_yearly` | Annual rainfall totals and year-over-year variations |
| | `rain_monthly_climatology` | Climatological monsoon rainfall distribution per calendar month |
| | `rain_location_ranking` | Precipitation volume rankings and wettest/driest station classification |
| | `rain_seasonal` | Monsoon vs post-monsoon seasonal precipitation distribution |
| | `rain_unusual_months` | Monthly deluge anomaly detection using standardized Z-Scores ($Z \ge 2.0$) |
| | `rain_heavy_days` | Frequency of heavy ($\ge 64.5\text{ mm}$) and very heavy ($\ge 115.6\text{ mm}$) rain days |
| **Climate** | `climate_year_profile` | Cross-variable annual summary (temperature, rain, humidity, wind, pressure) |
| | `climate_year_comparison` | Baseline vs comparison year delta analysis (e.g. 2015 vs 2020) |
| | `climate_city_profile` | Multi-parameter climatological station profile scorecard |
| | `extreme_thresholds` | Station 95th percentile hot/wet and 5th percentile cold baseline thresholds |
| | `extreme_events_yearly` | Frequency of threshold-breaching extreme events per calendar year |
| | `extreme_top_events` | Historical top-N extreme meteorological records across all observations |
| | `heatwave_spells` | Multi-day heatwave identification using gaps-and-islands windowing |
| | `correlations` | Pearson correlation matrix across atmospheric variables (raw and deseasonalized) |
| **KPIs** | `kpi_summary` | Executive headline scorecard metrics (46,752 records, record highs/lows) |

---

## ⚡ Performance Summary (Cold-Path vs Warm Cache)

Timings measured headlessly using Streamlit `AppTest` on real historical weather observations (46,752 records, 8 stations):

| Interaction / Action | Cold (Prefetch OFF) | Warm Cache | Target | Status |
| :--- | :---: | :---: | :---: | :--- |
| **`prepare()` on primary dataset** | **3.943 s** | -- | $\le 12.0\text{ s}$ | **PASS** |
| **Time to 10 KPI cards visible** | **5.376 s** | **0.120 s** | $\le 3.0\text{ s}$ (warm: $\le 0.5\text{ s}$) | **PASS** |
| **Single analysis view (`temp_yearly`)** | **0.073 s** | **0.065 s** | $\le 3.0\text{ s}$ | **PASS** |
| **Apply new filter (city = Mumbai)** | **3.666 s** | **0.210 s** | $\le 3.0\text{ s}$ | **PASS** |
| **Section: Overview** | **5.376 s** | **0.340 s** | $\le 5.0\text{ s}$ | **PASS** |
| **Section: Temperature** | **2.954 s** | **0.380 s** | $\le 5.0\text{ s}$ | **PASS** |
| **Section: Rainfall** | **4.793 s** | **0.395 s** | $\le 5.0\text{ s}$ | **PASS** |
| **Section: Climate patterns** | **7.595 s** | **0.310 s** | $\le 5.0\text{ s}$ | **PASS** |
| **Section: Data quality audit** | **0.039 s** | **0.030 s** | $\le 1.0\text{ s}$ | **PASS** |

### Distributed Scale Verification
A 20x replication benchmark (`python scripts/scale_test.py`) generating **935,040 rows** verified that all 22 Spark SQL analytical queries execute end-to-end in **14.12 seconds** without modifying a single line of SQL.

---

## 🧪 Comprehensive Test Suite

Run the full automated test suite and individual verification suites:

```bash
# 1. Full Pytest Suite (154+ passing tests)
pytest -v

# 2. UI Smoke & AppTest Lifecycle Suite
pytest tests/test_app_smoke.py -v

# 3. Dataset Variant Invariant Suite (V1 through V10)
pytest tests/test_variants.py -v

# 4. Large-Scale 20x Distributed Emulation Test (~935k rows)
python scripts/scale_test.py

# 5. Measure UI Latency & End-to-End Timings
python tools/measure_ui_timing.py
```

---

## 🛠️ Verification & Diagnostic Tools

Key user entry scripts located in `scripts/`:
- `scripts/verify_environment.py`: Validates Python, Java JDK, PySpark, and memory limits.
- `scripts/download_weather_data.py`: Downloads or verifies the 8-city historical dataset.
- `scripts/profile_dataset.py`: Profiles row counts, column coverage, and value distributions.
- `scripts/ingest_dataset.py`: Ingests and standardizes arbitrary weather CSVs.
- `scripts/clean_dataset.py`: Runs the deterministic data cleaning and linear interpolation pipeline.
- `scripts/run_analysis.py`: Command-line execution runner for all 22 Spark SQL analyses.
- `scripts/make_variant_files.py`: Generates the 10 standard edge-case variant datasets.
- `scripts/scale_test.py`: 20x scale testing utility (~935,040 rows).

Internal audit, profiling, and benchmark scripts are located in `tools/`.

---

## 💻 Hardware Environment & Engineering Boundaries

- **Operating System**: Microsoft Windows 11
- **Runtime Environment**: Python 3.13.3 (64-bit), OpenJDK 21, PySpark 3.5.3, Streamlit 1.40.2, Plotly 5.24.1
- **Architecture Constraints**:
  - Zero Python UDFs, `pandas_udf`, or RDD operations (all analytics execute natively in Spark Catalyst SQL engine).
  - Pure Spark SQL windowing and arithmetic for gap interpolation (linear interpolation on gaps $\le 3$ days; rainfall is strictly never interpolated).
  - Zero synthetic weather data generated in analytical production code.
  - Strict privacy isolation: Zero personal names or absolute filesystem paths in repository assets.

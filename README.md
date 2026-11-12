# Weather and Climate Pattern Discovery using Spark SQL

A Big Data analytics project that uses **Apache Spark SQL** and **PySpark** as the
primary computation engine to discover weather and climate patterns from large CSV
datasets.  An interactive **Streamlit + Plotly** dashboard surfaces the insights.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Core engine | Apache Spark 3.5 / Spark SQL |
| Python wrapper | PySpark 3.5.3 |
| Supporting data lib | Pandas 2.2 |
| Dashboard | Streamlit 1.40 |
| Visualisation | Plotly 5.24 |
| Language | Python 3.11+ |
| Version control | Git / GitHub |

---

## Prerequisites

1. **Python 3.11 or 3.12** – <https://www.python.org/downloads/>
2. **Java 11 or 17** (required by Spark) –
   - Windows: install [Eclipse Temurin 17](https://adoptium.net/) and set
     `JAVA_HOME` to the JDK root (e.g. `%ProgramFiles%\Eclipse Adoptium\jdk-17...`).
   - macOS: `brew install --cask temurin@17`
   - Linux: `sudo apt install openjdk-17-jdk`

---

## Setup

```bash
# 1. Clone the repository
git clone <repo-url>
cd weather-climate-spark

# 2. Create a virtual environment
python -m venv .venv

# 3. Activate it
#    Windows PowerShell:
.venv\Scripts\Activate.ps1
#    macOS / Linux:
source .venv/bin/activate

# 4. Install dependencies
pip install -r requirements.txt

# 5. Verify the environment
python scripts/verify_environment.py

# 6. Run tests
pytest -v

# 7. Launch the dashboard (placeholder)
streamlit run app.py
```

---

## Project Structure

```
weather-climate-spark/
├── app.py                  Streamlit dashboard entry point
├── requirements.txt        Pinned Python dependencies
├── README.md               This file
├── .gitignore
├── config/
│   ├── settings.py         Project-wide constants and paths
│   └── schema.py           Canonical Spark SQL StructType schema & aliases
├── data/
│   ├── raw/                Drop CSV source files here (india_weather_daily.csv tracked)
│   ├── uploads/            Destination for sanitized uploaded files
│   └── processed/          Spark-processed outputs (not committed)
├── src/
│   ├── __init__.py
│   ├── spark_session.py    SparkSession factory with auto-JAVA_HOME detection
│   ├── utils.py            Cross-platform Spark path resolution and display
│   ├── upload.py           Safe file upload handling & path traversal protection
│   ├── data_loader.py      CSV ingestion, date detection & schema standardization
│   ├── validator.py        Spark SQL validation & quality profiling
│   ├── cleaner.py          Pure Spark SQL data cleaning pipeline & audit tracking
│   └── queries/            Spark SQL query modules (Phase 5+)
├── tests/
│   ├── conftest.py         Shared session-scoped Spark fixture & real data slices
│   ├── test_environment.py Pytest environment checks
│   ├── test_schema.py      Schema, column alias, and mapping unit tests
│   ├── test_dataset.py     Dataset integrity and plausibility tests
│   ├── test_upload.py      Upload sanitization, security & constraint tests
│   ├── test_ingestion.py   End-to-end ingestion, mapping & validation tests
│   └── test_cleaning.py    Exhaustive data cleaning pipeline & gap interpolation tests
├── scripts/
│   ├── verify_environment.py    One-shot environment verifier
│   ├── diagnose_spark_features.py Diagnostic runner for PySpark capabilities
│   ├── download_weather_data.py  Acquires real ERA5 daily observations
│   ├── profile_dataset.py        Spark SQL structural and statistical profiler
│   ├── ingest_dataset.py         CLI demo for dataset ingestion and validation
│   └── clean_dataset.py          CLI driver for Spark SQL cleaning pipeline
├── docs/
│   ├── DATASET.md          Provenance, schema dictionary, and atmospheric notes
│   ├── dataset_profile.txt Generated output from Spark SQL profiling
│   └── screenshots/        Dashboard screenshots for the report
└── notebooks/              Exploratory Jupyter notebooks
```

---

## Dataset

The primary dataset covers **16 years (2010-01-01 to 2025-12-31)** of daily meteorological observations across 8 diverse geographical centers in India: Bengaluru, Mumbai, Delhi, Chennai, Kolkata, Hyderabad, Jaipur, and Shillong (46,752 records total). Data originates from the **Open-Meteo Historical Weather API (ERA5 Reanalysis)** and is licensed under Creative Commons Attribution 4.0 International (CC BY 4.0).

### Download the Dataset
```bash
python scripts/download_weather_data.py
```
Options:
* `--output`: Target CSV path (defaults to `data/raw/india_weather_daily.csv`)
* `--start`: Start date YYYY-MM-DD (default: `2010-01-01`)
* `--end`: End date YYYY-MM-DD (default: `2025-12-31`)
* `--force`: Overwrite existing output file

### Profile the Dataset with Spark SQL
```bash
python scripts/profile_dataset.py
```
This loads the dataset into a Spark SQL temporary view (`weather_raw`) under strict `FAILFAST` mode with `STANDARD_SCHEMA`, validates column order, checks null counts, validates primary key uniqueness (`date`, `location`), checks physical plausibility ranges, and saves the full report to `docs/dataset_profile.txt`.

---

## Ingestion and Validation (Phase 3)

The ingestion engine allows the system to accept arbitrary real-world daily weather CSV files, automatically map varied headers, detect delimiters and date formats, project columns into the canonical `STANDARD_SCHEMA`, and perform an exhaustive Spark SQL quality audit without modifying or synthesizing any records.

### Key Capabilities
* **Automatic Delimiter Detection**: Supports comma (`,`), semicolon (`;`), tab (`\t`), and pipe (`|`), with transparent UTF-8 BOM (`\xef\xbb\xbf`) handling.
* **Intelligent Column Mapping**: Normalizes raw column headers, matches against extensive domain aliases (`COLUMN_ALIASES`), and prevents duplicate assignment.
* **Pure Spark SQL Date Format Detection**: Evaluates candidates (`yyyy-MM-dd`, `dd-MM-yyyy`, `MM-dd-yyyy`, `yyyy/MM/dd`, `dd/MM/yyyy`, `MM/dd/yyyy`, `dd.MM.yyyy`, `yyyyMMdd`) in a single SQL query, reports parse rates, and flags ambiguities (e.g. day-first vs month-first when all days ≤ 12).
* **Safe Standardization**: Projects data into canonical types via Spark SQL `to_date()` and `try_cast(... AS DOUBLE)` without crashing on unexpected string values.
* **Secure Upload Handler**: Enforces strict extension checking (`.csv`), file size limits (≤ 200 MB), non-empty validation, timestamped naming, and complete path traversal protection.

### Quality & Plausibility Validation Checks
The validator runs purely inside Spark SQL and produces structured errors, warnings, column stats, and SQL query audit logs:
1. **Required Columns Check**: Ensures `date`, `location`, `temperature`, `rainfall`, `humidity`, `wind_speed`, and `pressure` are mapped (or `location` has a provided default).
2. **Row Count Thresholds**: Fails if dataset has 0 rows; warns if row count < 30.
3. **Date Parse Reliability**: Fails if parse rate < 50%; warns if any rows have unparseable dates; extracts date span.
4. **Numeric Parsing Integrity**: Warns if raw non-empty cells fail numeric casting.
5. **Null Proportion Audit**: Warns if null rates exceed 20% on any mapped variable.
6. **Sentinel Code Detection**: Warns if legacy weather missing codes (`-999`, `-9999`, `9999`) are present.
7. **Physical Plausibility Limits**: Evaluates domain boundaries (e.g. temperature [-60°C, 60°C], humidity [0%, 100%], rainfall [0 mm, 2000 mm]). Warns if values exceed bounds, flagging likely unit mismatches if > 50% are out-of-range.
8. **Primary Key Uniqueness**: Checks for duplicate `(date, location)` combinations.
9. **Location Completeness**: Computes row count, date span, and missing observation days for up to 20 locations.
10. **Numeric Summary Statistics**: Computes min, avg, max, and valid counts for all numerical columns.

### Ingestion CLI Usage
```bash
# Ingest default primary dataset
python scripts/ingest_dataset.py

# Ingest custom CSV with automatic mapping
python scripts/ingest_dataset.py --file path/to/dataset.csv

# Override or specify column mappings and date format explicitly
python scripts/ingest_dataset.py --file custom_weather.csv --map temp=temperature --map rain=rainfall --date-format dd/MM/yyyy

# Provide default location for single-station datasets lacking a location column
python scripts/ingest_dataset.py --file single_station.csv --location-name "Bengaluru"
```

#### CLI Exit Codes
* `0`: Validation passed (0 errors; data is clean and ready for analysis).
* `1`: Ingestion succeeded, but validation errors were detected (e.g. missing required column, unparseable dates).
* `2`: Fatal file loading or configuration error (file not found, duplicate CSV header names, empty file).

---

## Data Cleaning Pipeline (Phase 4)

The cleaning pipeline (`src/cleaner.py`) turns validation findings into precise, audit-tracked transformations implemented **entirely in Spark SQL**. Raw input datasets are strictly immutable and never modified or overwritten.

### 7-Step Pure Spark SQL Pipeline
1. **Step 1: Drop Unusable Records**: Drops rows where `date IS NULL`, `location IS NULL`, or `TRIM(location) = ''`.
2. **Step 2: Sentinel Code Removal**: Replaces missing-data indicator codes (`-999`, `-9999`, `9999`) with `NULL` across all numeric columns.
3. **Step 3: Plausible Physical Range Filtering**: Converts physical anomalies (e.g. humidity > 100%, negative wind speed, extreme pressure) to `NULL`. Optional `--drop-out-of-range` drops records containing temperature outliers instead of nulling.
4. **Step 4: Smart Deduplication on `(date, location)`**: Removes duplicate entries, retaining the record with the highest non-null metric count (breaking ties in favor of valid `rainfall`).
5. **Step 5: Short Gap Linear Interpolation**: Interpolates missing values for short gaps ($\le 3$ calendar days) per location over time using SQL window functions (`last()` and `first()` with `IGNORE NULLS`). 
   - **Crucial Rule**: `rainfall` is **NEVER** interpolated (rain is episodic; interpolation creates artificial rain).
   - Generates provenance flags: `<col>_was_filled` (`temperature_was_filled`, `humidity_was_filled`, etc.) marking filled values.
   - Gaps $> 3$ days remain `NULL`.
6. **Step 6: Temporal Quality Flags**: Computes `year`, `month`, `day_of_year`, and Meteorological `season` (`Winter`: Jan-Feb, `Summer`: Mar-May, `Monsoon`: Jun-Sep, `Post-monsoon`: Oct-Dec) mapping all 12 calendar months.
7. **Step 7: Final Clean View Registration**: Registers the canonical temporary view `weather_clean` ordered by `location, date`, and compiles structured audit metrics (`CleaningReport`).

### Cleaning CLI Usage
```bash
# Run cleaning pipeline on default dataset (view in-memory audit table)
python scripts/clean_dataset.py

# Clean custom dataset with explicit mapping
python scripts/clean_dataset.py --file path/to/dataset.csv --map temp=temperature

# Clean and persist to data/processed/weather_clean.csv & cleaning_report.json
python scripts/clean_dataset.py --save

# Optional flags: disable interpolation or drop temperature outliers
python scripts/clean_dataset.py --no-fill --drop-out-of-range
```

---

## Analysis Engine (Phase 5)

The analysis engine executes 22 analytical queries using **pure Spark SQL** to discover long-term trends, seasonal climatologies, precipitation anomalies, and extreme weather events.

### Analytical Capabilities by Category

| Category | Query Key | Description | Key Spark SQL Technique |
|---|---|---|---|
| **Temperature** | `temp_yearly` | Annual averages, records, anomalies & YoY changes | Window `AVG() OVER (PARTITION BY location)`, `LAG()` |
| | `temp_monthly_climatology` | 12-month temperature normals | Multi-year `AVG(temperature)` grouped by month |
| | `temp_monthly_series` | Chronological monthly averages & anomalies | Window monthly baseline deviation |
| | `temp_location_ranking` | Thermal ranking with record max/min dates | `RANK() OVER (ORDER BY avg_temp DESC)`, conditional `FIRST()` |
| | `temp_trend` | Decadal OLS linear trend on complete years | `regr_slope()`, `regr_r2()`, complete year filter |
| | `temp_seasonal` | Seasonal temperatures & diurnal ranges | `avg_temp_max - avg_temp_min` by season |
| **Rainfall** | `rain_monthly` | Monthly totals & rainy days ($\ge 2.5\text{ mm}$) | IMD threshold conditional aggregation |
| | `rain_yearly` | Annual precipitation totals & rainy day counts | Multi-year sums and annual anomalies |
| | `rain_monthly_climatology` | Climatological precipitation distribution | Multi-year monthly precipitation normals |
| | `rain_location_ranking` | Wettest to driest location ranking | `RANK() OVER (ORDER BY avg_annual_rain DESC)` |
| | `rain_seasonal` | Seasonal rainfall distribution & percentage shares | Window `SUM() OVER (PARTITION BY location)` for annual shares |
| | `rain_unusual_months` | Wet month anomalies via multi-year Z-scores | `STDDEV_SAMP(total_rain)`, threshold $Z \ge 2.0$ |
| | `rain_heavy_days` | IMD heavy ($\ge 64.5\text{ mm}$), very heavy, extremely heavy | Multi-tier conditional counts |
| **Climate** | `climate_year_profile` | Multi-variate annual climate profile across all stations | Multi-station annual atmospheric aggregation |
| | `climate_year_comparison` | Head-to-head comparison between two calendar years | Dynamic metric unpivoting with safe division |
| | `climate_city_profile` | Comprehensive multi-station climatological baseline | Station geographical and atmospheric profiling |
| | `extreme_thresholds` | 95th percentile hot days, 5th cold, 95th wet days | `percentile_approx(..., 0.95, 10000)` on clean view |
| | `extreme_events_yearly` | Annual counts of hot, cold, and extreme rain days | Threshold comparison joined against baseline |
| | `extreme_top_events` | All-time top 10 hottest, coldest, and wettest days | `DENSE_RANK() OVER (ORDER BY ...)` unioned |
| | `heatwave_spells` | Consecutive heatwave spells ($\ge 3$ days) | SQL Gaps-and-Islands: `date - ROW_NUMBER()` |
| | `correlations` | Atmospheric variable correlation matrix | Native Spark `CORR(x, y)` (raw & deseasonalized) |
| **KPIs** | `kpi_summary` | Executive dashboard climate scorecard | Global multi-metric headline aggregation |

### Analysis CLI Usage
```bash
# Run all 22 analyses and display formatted tabular previews
python scripts/run_analysis.py

# Inspect Spark Catalyst execution plan (shows lazy evaluation and optimization)
python scripts/run_analysis.py --explain temp_trend

# Run specific query with station and temporal filters
python scripts/run_analysis.py --location Mumbai --start 2020-01-01 --end 2022-12-31 --only rain_yearly

# Execute and persist complete sanitized text report
python scripts/run_analysis.py --save-report
```

### Viva Talking Points
1. **Pure Spark SQL Transformation Engine**: Transformations, window calculations, and regressions execute strictly in JVM Spark SQL. Zero Python UDFs or RDD operations are used, preventing Python worker serialization overhead and Windows socket instability.
2. **Lazy Evaluation & Plan Optimization**: Calling an analysis returns a lazy `QueryResult` containing a Spark `DataFrame`. Computation is only executed when materialization occurs (e.g. `count()` or `show()`). Running `--explain <key>` displays the physical execution plan, showing projection pruning and push-down predicates.
3. **Advanced SQL Techniques**:
   - **Gaps-and-Islands**: `heatwave_spells` identifies consecutive streaks of hot days by computing `DATE_SUB(date, ROW_NUMBER() OVER (PARTITION BY location ORDER BY date))`. When days are consecutive, the difference remains invariant, grouping unbroken spells without recursive queries.
   - **Linear Regression**: `temp_trend` leverages Spark's built-in `regr_slope` and `regr_r2` over complete calendar years ($\ge 95\%$ coverage) to calculate decadal warming rates without importing external numerical libraries.
   - **Multi-Year Standardized Z-Scores**: `rain_unusual_months` computes sample standard deviation across historical years using `STDDEV_SAMP`, flagging precipitation events exceeding 2.0 standard deviations from normal.
4. **Safety & Governance**: User input is strictly escaped via `sql_string_literal` preventing SQL injection; `QueryResult.to_pandas()` enforces an explicit row threshold (`ANALYSIS_RESULT_MAX_ROWS = 5000`) protecting front-end memory.

---

## Dashboard (Phase 6)

The Streamlit dashboard foundation provides an interactive user interface driven exclusively by **Apache Spark SQL**.

### How to Launch
```bash
streamlit run app.py
```
*(On first load, Apache Spark initializes in the background and prepares/caches the clean dataset in approximately 20–30 seconds).*

### Dashboard Architecture & Features
* **Sidebar Controls**:
  - **Data Source**: Switch between the primary built-in 8-city historical dataset and custom CSV uploads.
  - **Upload & Column Mapping**: Upload custom daily weather CSVs (up to 200 MB), inspect detected delimiters and headers, map source fields to the 11 standard columns with collision prevention, and optionally provide fallback location names.
  - **Filters**: Multi-select stations, select bounded date ranges, filter specific years, and filter meteorological seasons. Empty location selections render informative warnings without error.
* **Top Metric Strip**: Displays the active dataset label, origin source (`BUILTIN` or `UPLOAD`), raw row count, clean row count, and count of linearly interpolated values.
* **Section-Based Navigation (Performance Design)**:
  - The dashboard uses horizontal `st.radio("Select View", options=SECTIONS, horizontal=True)` instead of `st.tabs`.
  - *Rationale*: Streamlit evaluates all tabs on every rerun unless guarded, which would trigger all 22 analytical Spark SQL queries on every filter update. Horizontal radio ensures that only the active view executes, keeping UI reruns fast and snappy.
* **Charts and Insights Architecture**:
  - **Interactive Plotly Visualizations**: Every analysis is rendered with dark slate styling matching `#1E293B`, responsive container width, and stable station colors across filter changes.
  - **Plotly Modebar Camera Icon**: One can capture presentation-ready PNG screenshots of any visualization by clicking the camera icon in the top-right corner of the Plotly chart toolbar.
  - **Spark SQL Inspection & Plan Explains**: Every chart container includes an expander with the exact Spark SQL query and parameters. The Spark SQL section provides physical Catalyst execution plans (`explain()`) for all 22 queries.
  - **Data Export**: Every analysis contains an interactive raw data table and a one-click CSV download button (`📥 Download {key}.csv`).
  - **Automated Meteorological Insights**: Deterministic, evidence-backed findings generated directly from Spark SQL metrics following 10 strict rules (clean language, no causal speculation, and numbers mirrored in evidence dictionaries).

### Section to Analysis Query Mapping

| Dashboard Section | Visualizations & Components | Spark SQL Query Keys |
|---|---|---|
| **Overview** | 10 Headline KPI metric cards, Key Findings insight cards, 4 headline visualizations | `kpi_summary`, `temp_yearly`, `rain_yearly`, `climate_city_profile`, `temp_trend` |
| **Temperature** | Annual anomalies bar chart, 12-month climatology cycle, station chronological series with 12m baseline, thermal ranking with extremes, least-squares warming trend, seasonal distribution | `temp_yearly`, `temp_monthly_climatology`, `temp_monthly_series`, `temp_location_ranking`, `temp_trend`, `temp_seasonal` |
| **Rainfall** | Annual precipitation with flagged incomplete years, year-month rainfall heatmap, monthly climatology, station volume ranking, seasonal monsoon breakdown, standardized anomaly (Z-Score $\ge 2.0$), IMD heavy deluge frequency | `rain_yearly`, `rain_monthly`, `rain_monthly_climatology`, `rain_location_ranking`, `rain_seasonal`, `rain_unusual_months`, `rain_heavy_days` |
| **Climate patterns** | Annual multi-variable atmospheric profile, benchmark year comparison (Year A vs B), multi-metric station ranking radar/bar, station 95th/5th percentile extreme thresholds, yearly extreme event counts, top extreme event leaderboard, heatwave spells (gaps-and-islands $\ge 3$ days), Pearson correlations (raw and anomaly modes) | `climate_year_profile`, `climate_year_comparison`, `climate_city_profile`, `extreme_thresholds`, `extreme_events_yearly`, `extreme_top_events`, `heatwave_spells`, `correlations` |
| **Data quality** | Validation status, critical error/warning callouts, defect metric scorecard, column completeness and statistical range table | Validation report (`load_result.validation`) |
| **Cleaning report** | Raw-to-clean row counts, step-by-step transformation metrics, row removal deltas, interpolation counts, individual cleaning SQL logs | Cleaning report (`cleaning_report`) |
| **Data preview** | Interactive clean dataset preview with user-configurable row limit (10 to 1,000 rows) and preview SQL query | `get_preview` on `weather_clean` |
| **Spark SQL** | Query selector across all 22 analyses, formatted SQL query display, and Catalyst optimizer physical execution plan generator (`explain(extended=True)`) | `get_analysis_frame`, Catalyst `queryExecution` |

* **Architecture & Memory Governance**:
  - `src/dashboard/services.py` contains zero `streamlit` imports, ensuring 100% independent unit testability.
  - `ActiveDatasetStore` manages the active session view. When switching datasets, it explicitly uncaches the old table (`spark.catalog.uncacheTable`) so the server never serves stale data.
  - **Single Active Dataset Limitation**: Because Spark temporary views are session-wide, the local application instance maintains one active dataset view at a time.
  - All cached UI queries (`st.cache_data`) are keyed by dataset version and hashable parameters, operating strictly on plain Python/Pandas structures without caching Spark DataFrames.
* **Known Acceptance Reference Values (Quick Correctness Check)**:
  - *All Data (No Filters)*: 46,752 records, avg temperature 24.6 °C, max temperature 46.6 °C at Jaipur on 2024-05-27, total rainfall 174,900.5 mm.
  - *Filtered (Mumbai, 2020-01-01 to 2022-12-31)*: 1,096 records, total rainfall 7,706.1 mm.
* **Testing Note**: Upload column mapping cannot be tested by AppTest due to Streamlit 1.40.2 file_uploader test harness limitations and was verified manually.

---

## Project Engineering Rules

Based on rigorous diagnostic testing in the Windows + Python 3.13 + PySpark 3.5.3 environment:

### What Works Reliably (Supported)
* **`spark.sql()`**: All Spark SQL statements (SELECT, GROUP BY, aggregates, window functions, CTEs, CASE statements).
* **`spark.read.csv()`**: Ingestion of CSV files using explicit `StructType` schemas and `to_spark_path()`.
* **`.toPandas()`**: Converting small, aggregated Spark SQL query results into Pandas DataFrames for Streamlit and Plotly visualizations.

### What Fails on this Machine (Prohibited)
* **RDD operations**: `sparkContext.parallelize()`, `rdd.map()`, etc. fail due to Windows loopback socket communication issues (`WinError 10038` / `EOFException`).
* **Python UDFs & pandas_udf**: Any Python worker sub-process execution.
* **`spark.createDataFrame()` from Python lists or Pandas DataFrames**: Spawns Python worker processes that crash.

### Golden Rule of Data Ingestion & Computation
1. Data enters Apache Spark **exclusively** through `spark.read.csv()` with canonical explicit schema.
2. All analytical transformations and business logic **must be written in Spark SQL**.
3. Small, final aggregated metric tables are retrieved via `.toPandas()` for front-end rendering.
4. **Never generate synthetic or mock weather data anywhere in the project.**

---

## Phases

| Phase | Description | Status |
|---|---|---|
| 1 | Environment setup & project skeleton | ✅ Complete |
| 1B | JAVA_HOME auto-detection & diagnostics | ✅ Complete |
| 2 | Dataset acquisition, standard schema & Spark SQL profiling | ✅ Complete |
| 3 | Ingestion, column mapping, validation & upload handling | ✅ Complete |
| 4 | Cleaning pipeline & Phase 3 evidence audit | ✅ Complete |
| 5 | Spark SQL pattern discovery & analysis queries | ✅ Complete |
| 6 | Streamlit dashboard foundation, column mapping & KPI scorecard | ✅ Complete |
| 7 | Interactive Plotly charts & auto-generated text insights | ✅ Complete |

---

## License

MIT – see `LICENSE` for details.


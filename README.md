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
│   └── queries/            Spark SQL query modules (Phase 4+)
├── tests/
│   ├── conftest.py         Shared session-scoped Spark fixture & real data slices
│   ├── test_environment.py Pytest environment checks
│   ├── test_schema.py      Schema, column alias, and mapping unit tests
│   ├── test_dataset.py     Dataset integrity and plausibility tests
│   ├── test_upload.py      Upload sanitization, security & constraint tests
│   └── test_ingestion.py   End-to-end ingestion, mapping & validation tests
├── scripts/
│   ├── verify_environment.py    One-shot environment verifier
│   ├── diagnose_spark_features.py Diagnostic runner for PySpark capabilities
│   ├── download_weather_data.py  Acquires real ERA5 daily observations
│   ├── profile_dataset.py        Spark SQL structural and statistical profiler
│   └── ingest_dataset.py         CLI demo for dataset ingestion and validation
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
| 4 | Spark SQL analysis & pattern discovery | 🔜 Planned |
| 5 | Streamlit dashboard & visualizations | 🔜 Planned |

---

## License

MIT – see `LICENSE` for details.


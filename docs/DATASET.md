# Primary Dataset: India Daily Weather (2010–2025)

## 1. Overview and Provenance

This project utilizes real-world historical daily meteorological records covering eight major metropolitan and climatic zones across India.

* **Provider**: [Open-Meteo](https://open-meteo.com) Historical Weather API
* **Underlying Model**: ECMWF ERA5 Reanalysis and ERA5-Land (European Centre for Medium-Range Weather Forecasts)
* **API Endpoint**: `https://archive-api.open-meteo.com/v1/archive`
* **Acquisition Method**: Direct `daily` aggregated archive queries
* **Timezone**: `Asia/Kolkata` (UTC+05:30)
* **Temporal Coverage**: `2010-01-01` to `2025-12-31` (16 full calendar years / 5,844 consecutive days per city)
* **Total Volume**: 46,752 records across 8 geographical locations

---

## 2. Geographic Coverage

The dataset samples eight distinct Indian agro-ecological and meteorological regions:

| City | State / Region | Climate Type | Latitude (°N) | Longitude (°E) | Records |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Bengaluru** | Karnataka (Deccan Plateau) | Semi-arid / Tropical Savanna | 12.9716 | 77.5946 | 5,844 |
| **Mumbai** | Maharashtra (West Coast) | Tropical Wet and Dry | 19.0760 | 72.8777 | 5,844 |
| **Delhi** | National Capital Region (North Plains) | Hot Semi-Arid / Subtropical | 28.6139 | 77.2090 | 5,844 |
| **Chennai** | Tamil Nadu (Coromandel Coast) | Tropical Wet and Dry (Northeast Monsoon) | 13.0827 | 80.2707 | 5,844 |
| **Kolkata** | West Bengal (Ganges Delta) | Tropical Wet-and-Dry | 22.5726 | 88.3639 | 5,844 |
| **Hyderabad** | Telangana (Central Deccan) | Tropical Wet and Dry | 17.3850 | 78.4867 | 5,844 |
| **Jaipur** | Rajasthan (Thar Desert Border) | Hot Semi-Arid | 26.9124 | 75.7873 | 5,844 |
| **Shillong** | Meghalaya (Eastern Himalayas) | Subtropical Highland (Monsoon Heavy) | 25.5788 | 91.8933 | 5,844 |

---

## 3. Data Dictionary

The canonical storage format in `data/raw/india_weather_daily.csv` follows this exact column schema:

| Column | Spark SQL Type | Unit | Nullable | Description |
| :--- | :--- | :--- | :--- | :--- |
| `date` | `DateType` | `YYYY-MM-DD` | No | Calendar date of meteorological observation |
| `location` | `StringType` | — | No | Standard city name |
| `latitude` | `DoubleType` | °N | Yes | Latitude of observing station |
| `longitude` | `DoubleType` | °E | Yes | Longitude of observing station |
| `temperature` | `DoubleType` | °C | Yes | Daily mean 2-metre air temperature |
| `temp_max` | `DoubleType` | °C | Yes | Daily maximum 2-metre air temperature |
| `temp_min` | `DoubleType` | °C | Yes | Daily minimum 2-metre air temperature |
| `rainfall` | `DoubleType` | mm | Yes | Daily aggregated precipitation liquid equivalent |
| `humidity` | `DoubleType` | % | Yes | Daily mean relative humidity at 2 metres |
| `wind_speed` | `DoubleType` | km/h | Yes | Daily mean wind speed at 10 metres above ground |
| `pressure` | `DoubleType` | hPa | Yes | Mean sea-level barometric pressure |

### Atmospheric Pressure Note
Atmospheric pressure is acquired as **Mean Sea Level Pressure** (`pressure_msl_mean`, hPa). Surface air pressure drops exponentially with elevation (~12 hPa per 100 metres). Because our geographical stations range from sea level (Mumbai: ~14 m, Chennai: ~6 m) to high plateaus and mountains (Bengaluru: ~920 m, Shillong: ~1,525 m), raw surface pressure cannot be compared across locations. Sea-level adjusted pressure standardizes readings to a common reference plane, making barometric gradients, depressions, and monsoon troughs physically comparable across all cities.

---

## 4. License and Attribution

* **Data Attribution**: *Weather data by Open-Meteo.com*
* **License**: Open-Meteo API data is licensed under the [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/) license.
* **Copernicus ERA5 Attribution**: Contains modified Copernicus Climate Change Service information (2024). Neither the European Commission nor ECMWF is responsible for any use that may be made of the information it contains.


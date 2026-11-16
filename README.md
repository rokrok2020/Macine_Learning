Group: <number>

# Machine Learning 364-1-1811 - Project Part A
Binary classification of **RainTomorrow**: will at least 1.0 mm of precipitation fall
tomorrow, in one of 6 cities, based on today's daily weather from the
Open-Meteo Historical Weather API.

## Project structure
```
.
├── data_collection.py        # Step 1: download raw JSON from the API
├── sensing.py                # Step 2: merge local JSON -> sensed_data.csv (no API calls)
├── dataset_creation.ipynb    # Step 3: analysis, data quality, preparation, RainTomorrow
├── data/
│   ├── raw/
│   │   ├── <city>/<city>_<start>_<end>.json  # raw API responses, one folder per city
│   │   └── sensed_data.csv                    # one row per (location, date)
│   └── processed/
│       └── final_dataset.csv               # prepared data + RainTomorrow
├── requirements.txt
└── README.md
```

## Pipeline
```
Open-Meteo API -> data_collection.py -> data/raw/<city>/*.json
               -> sensing.py         -> data/raw/sensed_data.csv
               -> dataset_creation.ipynb -> data/processed/final_dataset.csv
```

## Files
| File | Role |
|---|---|
| `data_collection.py` | One GET per (location, calendar year) from 2010-01-01 until 7 days before the run date, saved to `data/raw/<city>/`. Saves each response byte-for-byte via a temp file, so an interrupted run never leaves a broken JSON. Skips files that already exist, so nothing is downloaded twice. Counts requests and waits before 5,000/hour or 10,000/24h. Retries on HTTP 429, 5xx and network errors; stops on HTTP 400 (the request must be fixed). If the API asks to wait more than 15 minutes (daily quota used up), it stops and can simply be re-run later. |
| `sensing.py` | Reads all `data/raw/<city>/*.json`, identifies the location from the coordinates inside the JSON (nearest city), adds `location` and `date`, removes duplicates by `location + date`, saves `data/raw/sensed_data.csv`. Makes no API calls. |
| `dataset_creation.ipynb` | Data understanding, prior probabilities, feature relationships, data quality checks, data preparation and creation of `RainTomorrow`. Saved with outputs and figures. |
| `requirements.txt` | Pinned package versions. |

## Data
- **Locations:** London, New York, Sydney, Berlin, Tokyo, Vancouver.
- **Required daily variables (10):** `weather_code`, `temperature_2m_max`, `temperature_2m_min`,
  `temperature_2m_mean`, `apparent_temperature_mean`, `precipitation_sum`, `rain_sum`,
  `precipitation_hours`, `wind_speed_10m_max`, `wind_direction_10m_dominant`.
- **Extra daily variables (8):** `relative_humidity_2m_mean`, `dew_point_2m_mean`,
  `pressure_msl_mean`, `cloud_cover_mean`, `wind_gusts_10m_max`, `sunshine_duration`,
  `shortwave_radiation_sum`, `et0_fao_evapotranspiration`.
- **Target:** within each location, sorted by date,
  `RainTomorrow = 1` if the next day's `precipitation_sum >= 1.0` mm, else `0`.
  The last day of each location is removed. No next-day information is used as a feature.

## How to run (Windows PowerShell, Python 3.11)
```powershell
python -m pip install -r requirements.txt
python data_collection.py                  # downloads only missing files
python sensing.py                          # builds data/raw/sensed_data.csv
jupyter notebook dataset_creation.ipynb    # Kernel -> Restart & Run All, then save
```
Close Jupyter before `pip install`, otherwise Windows may lock files.
The first full download is slow: Open-Meteo returns HTTP 429 at its per-minute limit,
and the script waits and continues automatically.

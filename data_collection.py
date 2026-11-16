"""Collect daily weather data from the Open-Meteo Historical Weather API.

- One request per (location, calendar year); each response is saved to
  data/raw/<location>_<start>_<end>.json exactly as received (raw bytes).
- Files that already exist are skipped, so re-running never downloads twice.
- Requests are counted and the loop waits before exceeding
  5,000 requests/hour or 10,000 requests/24h.
- HTTP 429 and 5xx (and network errors): wait and retry.
- HTTP 400: the request itself is wrong, so stop and report (no retry).
"""

from __future__ import annotations

import time
from collections import deque
from datetime import date, timedelta
from pathlib import Path

import requests

ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"
RAW_DIR = Path("data/raw")

HOURLY_LIMIT = 5_000
DAILY_LIMIT = 10_000
MAX_RETRIES = 10
REQUEST_TIMEOUT = 60
# Open-Meteo weights long/multi-variable requests as several calls,
# so a short pause avoids hitting the per-minute limit (HTTP 429).
PAUSE_BETWEEN_REQUESTS = 5

START_DATE = date(2010, 1, 1)
# The archive is published with a delay of a few days.
END_DATE = date.today() - timedelta(days=7)

LOCATIONS = {
    "london": (51.5074, -0.1278),
    "new_york": (40.7128, -74.0060),
    "sydney": (-33.8688, 151.2093),
    "berlin": (52.5200, 13.4050),
    "tokyo": (35.6762, 139.6503),
    "vancouver": (49.2827, -123.1207),
}

# The 10 variables required by the assignment.
REQUIRED_VARIABLES = [
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "apparent_temperature_mean",
    "precipitation_sum",
    "rain_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
    "wind_direction_10m_dominant",
]

# Optional extra variables, physically related to rain formation.
EXTRA_VARIABLES = [
    "relative_humidity_2m_mean",
    "dew_point_2m_mean",
    "pressure_msl_mean",
    "cloud_cover_mean",
    "wind_gusts_10m_max",
    "sunshine_duration",
    "shortwave_radiation_sum",
    "et0_fao_evapotranspiration",
]

DAILY_VARIABLES = REQUIRED_VARIABLES + EXTRA_VARIABLES


class RateLimiter:
    """Counts requests in sliding windows and sleeps before a limit is exceeded."""

    def __init__(self) -> None:
        self.hourly: deque[float] = deque()
        self.daily: deque[float] = deque()

    def wait(self) -> None:
        while True:
            now = time.time()
            while self.hourly and now - self.hourly[0] >= 3600:
                self.hourly.popleft()
            while self.daily and now - self.daily[0] >= 86400:
                self.daily.popleft()

            waits = []
            if len(self.hourly) >= HOURLY_LIMIT:
                waits.append(3600 - (now - self.hourly[0]))
            if len(self.daily) >= DAILY_LIMIT:
                waits.append(86400 - (now - self.daily[0]))
            if not waits:
                return
            time.sleep(max(waits) + 1)

    def record(self) -> None:
        now = time.time()
        self.hourly.append(now)
        self.daily.append(now)


def year_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into calendar-year chunks."""
    chunks = []
    for year in range(start.year, end.year + 1):
        chunk_start = max(start, date(year, 1, 1))
        chunk_end = min(end, date(year, 12, 31))
        chunks.append((chunk_start, chunk_end))
    return chunks


def fetch(session: requests.Session, limiter: RateLimiter, params: dict) -> bytes:
    """Perform one GET with retries for 429/5xx/network errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        limiter.wait()
        limiter.record()
        try:
            response = session.get(ENDPOINT, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            print(f"  network error ({exc}), retry {attempt}/{MAX_RETRIES}")
            time.sleep(2 ** attempt)
            continue

        if response.status_code == 200:
            payload = response.json()
            if payload.get("error") or "daily" not in payload:
                raise RuntimeError(f"Invalid payload: {payload}")
            return response.content

        if response.status_code == 400:
            # Bad request: retrying the same request cannot succeed.
            raise ValueError(f"HTTP 400 - fix the request: {response.text}")

        if response.status_code == 429 or response.status_code >= 500:
            wait_s = int(response.headers.get("Retry-After", min(2 ** attempt * 15, 900)))
            print(f"  HTTP {response.status_code}, waiting {wait_s}s ({attempt}/{MAX_RETRIES})")
            time.sleep(wait_s)
            continue

        response.raise_for_status()

    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {params}")


def collect() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    limiter = RateLimiter()
    downloaded = skipped = 0

    with requests.Session() as session:
        for location, (lat, lon) in LOCATIONS.items():
            for chunk_start, chunk_end in year_chunks(START_DATE, END_DATE):
                out = RAW_DIR / f"{location}_{chunk_start}_{chunk_end}.json"
                if out.exists():
                    skipped += 1
                    continue

                params = {
                    "latitude": lat,
                    "longitude": lon,
                    "start_date": chunk_start.isoformat(),
                    "end_date": chunk_end.isoformat(),
                    "daily": ",".join(DAILY_VARIABLES),
                    "timezone": "auto",
                }
                out.write_bytes(fetch(session, limiter, params))
                downloaded += 1
                print(f"Saved {out}")
                time.sleep(PAUSE_BETWEEN_REQUESTS)

    print(f"Done. downloaded={downloaded}, skipped (already on disk)={skipped}")


if __name__ == "__main__":
    collect()

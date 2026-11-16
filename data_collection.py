"""Step 1 of the pipeline: collect daily weather data from the Open-Meteo
Historical Weather API and store the raw responses.

Design decisions (each one maps to a requirement of the assignment):
- Chunking: one GET request per (location, calendar year). 6 cities x 17 years
  (2010 - today) = 102 requests. Yearly chunks keep each response small and
  let an interrupted run resume from the last missing year.
- Storage: each response is saved to data/raw/<city>/<city>_<start>_<end>.json
  exactly as received from the server (raw bytes, no parsing / re-writing).
- Caching: a file that already exists is skipped, so re-running the script
  never downloads the same data twice.
- Rate limiting: every request (including retries) is counted in two sliding
  windows, and the loop sleeps before exceeding 5,000 requests/hour or
  10,000 requests/24h.
- Error handling:
    HTTP 429 / 5xx / network error -> temporary problem: wait, then retry.
    HTTP 400 -> the request itself is wrong: stop immediately (a retry of the
    same request can never succeed; the request must be fixed first).

Run:  python data_collection.py
"""

from __future__ import annotations

import time
from collections import deque
from datetime import date, timedelta
from pathlib import Path

import requests

# --- Configuration -----------------------------------------------------------

ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"
RAW_DIR = Path("data/raw")

HOURLY_LIMIT = 5_000
DAILY_LIMIT = 10_000
MAX_RETRIES = 10
REQUEST_TIMEOUT = 60
# Open-Meteo weights long/multi-variable requests as several calls,
# so a short pause avoids hitting the per-minute limit (HTTP 429).
PAUSE_BETWEEN_REQUESTS = 5
# A Retry-After longer than this means the daily quota is used up.
MAX_WAIT_SECONDS = 15 * 60

# Long history (~17 years) gives the model many examples of every season in
# every city. The model will be tested on future dates, so recent years matter.
START_DATE = date(2010, 1, 1)
# The archive is published with a delay of a few days.
END_DATE = date.today() - timedelta(days=7)

# City name -> (latitude, longitude). The API snaps these to the nearest grid cell.

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

# Optional extra variables (allowed by the assignment). Each one is physically
# related to rain formation: moisture (humidity, dew point), weather systems
# (pressure), clouds / sunshine / radiation, wind gusts (storms), evaporation.
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


# --- Request handling --------------------------------------------------------


class QuotaExhausted(Exception):
    """Raised when the server asks us to wait longer than MAX_WAIT_SECONDS.

    Instead of blocking for hours, the run stops; thanks to caching, the next
    run simply continues from the first missing file.
    """

    def __init__(self, wait_seconds: int) -> None:
        super().__init__(f"Quota exhausted, Retry-After={wait_seconds}s")
        self.wait_seconds = wait_seconds


class RateLimiter:
    """Counts requests in sliding windows and sleeps before a limit is exceeded."""

    def __init__(self) -> None:
        # Timestamps of requests sent in the last hour / last 24 hours.
        self.hourly: deque[float] = deque()
        self.daily: deque[float] = deque()

    def wait(self) -> None:
        """Block until one more request is allowed in both windows."""
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
        """Register a request that is about to be sent (retries count too)."""
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
    """Send one GET request and return the raw response bytes.

    Retries on 429 / 5xx / network errors with growing waits (or the server's
    Retry-After header). Stops immediately on 400.
    """
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
            # Validate the content, but return the original bytes unchanged.
            payload = response.json()
            if payload.get("error") or "daily" not in payload:
                raise RuntimeError(f"Invalid payload: {payload}")
            return response.content

        if response.status_code == 400:
            # Bad request: retrying the same request cannot succeed.
            raise ValueError(f"HTTP 400 - fix the request: {response.text}")

        if response.status_code == 429 or response.status_code >= 500:
            wait_s = int(response.headers.get("Retry-After", min(2 ** attempt * 15, 900)))
            if wait_s > MAX_WAIT_SECONDS:
                # Daily quota exhausted: stop instead of blocking for hours.
                raise QuotaExhausted(wait_s)
            print(f"  HTTP {response.status_code}, waiting {wait_s}s ({attempt}/{MAX_RETRIES})")
            time.sleep(wait_s)
            continue

        response.raise_for_status()

    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {params}")


# --- Main loop ---------------------------------------------------------------


def collect() -> None:
    """Download every missing (city, year) file into data/raw/<city>/."""
    limiter = RateLimiter()
    downloaded = skipped = 0

    with requests.Session() as session:
        for location, (lat, lon) in LOCATIONS.items():
            city_dir = RAW_DIR / location
            city_dir.mkdir(parents=True, exist_ok=True)
            for chunk_start, chunk_end in year_chunks(START_DATE, END_DATE):
                out = city_dir / f"{location}_{chunk_start}_{chunk_end}.json"
                if out.exists():  # caching: never download the same file twice
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
                try:
                    content = fetch(session, limiter, params)
                except QuotaExhausted as exc:
                    hours = exc.wait_seconds / 3600
                    print(f"API quota exhausted (retry in ~{hours:.1f}h). "
                          f"downloaded={downloaded}, skipped={skipped}. "
                          "Run again later - existing files are kept.")
                    return
                # Write to a temp file and rename, so a crash never leaves a broken JSON.
                tmp = out.with_suffix(".json.part")
                tmp.write_bytes(content)
                tmp.replace(out)
                downloaded += 1
                print(f"Saved {out}")
                time.sleep(PAUSE_BETWEEN_REQUESTS)

    print(f"Done. downloaded={downloaded}, skipped (already on disk)={skipped}")


if __name__ == "__main__":
    collect()

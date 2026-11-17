"""Step 1 of the pipeline: collect daily weather data from the Open-Meteo
Historical Weather API and store the raw responses.

<<<<<<< Updated upstream
Design decisions (each one maps to a requirement of the assignment):
- Chunking: one GET request per (location, calendar year). 6 cities x 17 years
  (2010-01-01 to 2026-11-09) = 102 requests. Yearly chunks keep each response small and
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
=======
- One request per (location, calendar year); each response is saved to
  data/raw/<location>_<start>_<end>.json exactly as received (raw bytes).
- Files that already exist and are complete are skipped, so re-running never
  downloads twice. Each file is written to a .tmp file first and renamed only
  when complete, so an interrupted run never leaves a broken file behind.
- Requests are counted (retries included) and the loop waits before exceeding
  5,000 requests/hour or 10,000 requests/24h.
- HTTP 429 and 5xx (and network errors): wait and retry.
- HTTP 400: the request itself is wrong, so stop and report (no retry).
>>>>>>> Stashed changes
"""

from __future__ import annotations

import json
import time
from collections import deque
from datetime import date
from pathlib import Path

import requests

# --- Configuration -----------------------------------------------------------

ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"
BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"

HOURLY_LIMIT = 5_000
DAILY_LIMIT = 10_000
MAX_RETRIES = 10
REQUEST_TIMEOUT = 60
MAX_WAIT_SECONDS = 3600
# Open-Meteo weights long/multi-variable requests as several calls,
# so a short pause avoids hitting the per-minute limit (HTTP 429).
PAUSE_BETWEEN_REQUESTS = 5
# A Retry-After longer than this means the daily quota is used up.
MAX_WAIT_SECONDS = 15 * 60

<<<<<<< Updated upstream
# Long history (~17 years) gives the model many examples of every season in
# every city. The model will be tested on future dates, so recent years matter.
START_DATE = date(2010, 1, 1)
# Fixed end date, so every run reproduces exactly the same dataset.
# (The archive is published with a delay of a few days, so the end date was
# set about a week before the collection date, 2026-11-16.)
END_DATE = date(2026, 11, 9)

# City name -> (latitude, longitude). The API snaps these to the nearest grid cell.
=======
# Fixed dates, so every run reproduces exactly the same dataset.
# (The archive is published with a delay of a few days, so the end date
# was set about a week before the collection date.)
START_DATE = date(2010, 1, 1)
END_DATE = date(2026, 11, 9)
>>>>>>> Stashed changes

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

<<<<<<< Updated upstream
# ASSUMPTION: the raw test files of Part B will contain these extra variables too
# (question sent to the course staff). If not, only REQUIRED_VARIABLES are used.
# Optional extra variables (allowed by the assignment). Each one is physically
# related to rain formation: moisture (humidity, dew point), weather systems
# (pressure), clouds / sunshine / radiation, wind gusts (storms), evaporation.
=======
# Optional extra variables, physically related to rain formation.
# ASSUMPTION: the raw test JSON files of Part B will include these variables
# as well. This was sent to the course staff for confirmation; if the answer
# is no, the models will use only REQUIRED_VARIABLES.
>>>>>>> Stashed changes
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
        self.total = 0

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
            print(f"  local limit reached, sleeping {max(waits):.0f}s")
            time.sleep(max(waits) + 1)

    def record(self) -> None:
        """Register a request that is about to be sent (retries count too)."""
        now = time.time()
        self.hourly.append(now)
        self.daily.append(now)
        self.total += 1


def year_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into calendar-year chunks."""
    chunks = []
    for year in range(start.year, end.year + 1):
        chunk_start = max(start, date(year, 1, 1))
        chunk_end = min(end, date(year, 12, 31))
        chunks.append((chunk_start, chunk_end))
    return chunks


def is_complete(payload: dict) -> bool:
    """True if the response has every requested variable, all with the same length."""
    if payload.get("error") or "daily" not in payload:
        return False
    daily = payload["daily"]
    if "time" not in daily:
        return False
    n_days = len(daily["time"])
    return all(var in daily and len(daily[var]) == n_days for var in DAILY_VARIABLES)


def already_downloaded(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        return is_complete(json.loads(path.read_text(encoding="utf-8")))
    except (json.JSONDecodeError, OSError):
        return False  # broken or partial file -> download it again


def retry_wait(response: requests.Response, attempt: int) -> int:
    """Seconds to wait: the server's Retry-After if it is a number, else exponential backoff."""
    retry_after = response.headers.get("Retry-After", "")
    if retry_after.isdigit():
        return min(int(retry_after), MAX_WAIT_SECONDS)
    return min(2 ** attempt * 15, MAX_WAIT_SECONDS)


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
            wait_s = min(2 ** attempt, 60)
            print(f"  network error ({exc.__class__.__name__}), retry {attempt}/{MAX_RETRIES} in {wait_s}s")
            time.sleep(wait_s)
            continue

        if response.status_code == 200:
<<<<<<< Updated upstream
            # Validate the content, but return the original bytes unchanged.
            payload = response.json()
            if payload.get("error") or "daily" not in payload:
                raise RuntimeError(f"Invalid payload: {payload}")
=======
            if not is_complete(response.json()):
                raise RuntimeError(f"Incomplete payload: {response.text[:300]}")
>>>>>>> Stashed changes
            return response.content

        if response.status_code == 400:
            # Bad request: retrying the same request cannot succeed.
            raise ValueError(f"HTTP 400 - fix the request: {response.text[:300]}")

        if response.status_code == 429 or response.status_code >= 500:
<<<<<<< Updated upstream
            retry_after = response.headers.get("Retry-After", "")
            wait_s = int(retry_after) if retry_after.isdigit() else min(2 ** attempt * 15, 900)
            if wait_s > MAX_WAIT_SECONDS:
                # Daily quota exhausted: stop instead of blocking for hours.
                raise QuotaExhausted(wait_s)
=======
            wait_s = retry_wait(response, attempt)
>>>>>>> Stashed changes
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
<<<<<<< Updated upstream
                out = city_dir / f"{location}_{chunk_start}_{chunk_end}.json"
                if out.exists():  # caching: never download the same file twice
=======
                out = RAW_DIR / f"{location}_{chunk_start}_{chunk_end}.json"
                if already_downloaded(out):
>>>>>>> Stashed changes
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
<<<<<<< Updated upstream
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
=======
                tmp = out.with_suffix(".tmp")
                tmp.write_bytes(fetch(session, limiter, params))
>>>>>>> Stashed changes
                tmp.replace(out)
                downloaded += 1
                print(f"Saved {out.name}")
                time.sleep(PAUSE_BETWEEN_REQUESTS)

    print(f"Done. downloaded={downloaded}, skipped (already on disk)={skipped}, "
          f"requests sent={limiter.total}")


if __name__ == "__main__":
    collect()

import csv
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

from logger_config import setup_logger

# Configuration
DB_FILE = Path("exchange_rates.csv")
START_DATE = date(2026, 1, 1)
CURRENCIES = ["USD", "EUR", "GBP"]          # UAH is handled separately (rate = 1)
NBU_API_URL = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange"

# Upsert rule: UPDATE if key already exists (explicit rule per the task)
UPSERT_RULE = "UPDATE"   # options: "UPDATE" | "SKIP"

# Retry settings
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
REQUEST_TIMEOUT = 15

# Logging
logger = setup_logger(__name__)

# CSV "database" helpers
COLUMNS = ["currency_name", "exchange_rate", "currency_code", "exchange_date", "updated_at"]


def load_db() -> dict[tuple[str, str], dict]:
    """Load the CSV into an in-memory dict keyed by (exchange_date, currency_code)."""
    records: dict[tuple[str, str], dict] = {}
    if not DB_FILE.exists():
        return records

    with DB_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["exchange_date"], row["currency_code"])
            records[key] = row
    logger.info(f"Loaded {len(records)} existing records from {DB_FILE}")
    return records


def save_db(records: dict[tuple[str, str], dict]) -> None:
    """Persist the in-memory dict back to CSV."""
    with DB_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        # Sort by date then currency for readability
        for key in sorted(records.keys()):
            writer.writerow(records[key])
    logger.info(f"Saved {len(records)} records to {DB_FILE}")


# Date gap detection
def find_missing_dates(
    records: dict[tuple[str, str], dict],
    currencies: list[str],
    start: date,
    end: date,
) -> list[date]:
    """
    Return a sorted list of dates in [start, end] that are missing
    at least one currency record.
    Weekends are included — NBU publishes rates on business days only,
    so missing weekend dates will be fetched and the API will return
    the last valid rate (or empty); we handle that gracefully.
    """
    missing: set[date] = set()
    current = start
    while current <= end:
        for code in currencies:
            if (str(current), code) not in records:
                missing.add(current)
                break
        current += timedelta(days=1)

    missing_sorted = sorted(missing)
    if missing_sorted:
        logger.info(f"Found {len(missing_sorted)} dates with missing records")
    return missing_sorted


# NBU API client
def fetch_rates(target_date: date) -> Optional[list[dict]]:
    """
    Fetch exchange rates from NBU API for a given date.
    Returns a list of rate dicts or None on persistent failure.

    Robustness measures:
    - Retry up to MAX_RETRIES times with exponential backoff
    - Validate HTTP status and content-type
    - Validate response is a non-empty list
    - Log and alert on empty / malformed responses
    """
    date_str = target_date.strftime("%Y%m%d")
    params = {"date": date_str, "json": ""}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                NBU_API_URL, params=params, timeout=REQUEST_TIMEOUT
            )

            if response.status_code != 200:
                logger.warning(
                    f"[{target_date}] Attempt {attempt}: HTTP {response.status_code}"
                )
                _maybe_retry(attempt)
                continue

            try:
                data = response.json()
            except json.JSONDecodeError as exc:
                logger.warning(
                    f"[{target_date}] Attempt {attempt}: Invalid JSON — {exc}"
                )
                _maybe_retry(attempt)
                continue

            # ── Known NBU edge-case: API returns empty list for non-business days
            #    or during maintenance windows. We treat this as a soft failure.
            if not isinstance(data, list) or len(data) == 0:
                logger.warning(
                    f"[{target_date}] Attempt {attempt}: Empty response from NBU API "
                    "(possible non-business day or API hiccup). Skipping date."
                )
                # Don't retry — empty response is a valid API reply for non-trading days
                return []

            logger.info(
                f"[{target_date}] Fetched {len(data)} currency records from NBU"
            )
            return data

        except requests.exceptions.Timeout:
            logger.warning(f"[{target_date}] Attempt {attempt}: Request timed out")
            _maybe_retry(attempt)
        except requests.exceptions.ConnectionError as exc:
            logger.warning(f"[{target_date}] Attempt {attempt}: Connection error — {exc}")
            _maybe_retry(attempt)
        except Exception as exc:
            logger.error(f"[{target_date}] Attempt {attempt}: Unexpected error — {exc}")
            _maybe_retry(attempt)

    logger.error(
        f"[{target_date}] All {MAX_RETRIES} attempts failed — ALERT: data missing for this date"
    )
    return None  # Persistent failure


def _maybe_retry(attempt: int) -> None:
    """Sleep with exponential backoff unless this is the last attempt."""
    if attempt < MAX_RETRIES:
        sleep_s = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
        logger.info(f"Retrying in {sleep_s}s …")
        time.sleep(sleep_s)


# Upsert logic
def upsert_record(
    records: dict[tuple[str, str], dict],
    exchange_date: date,
    currency_code: str,
    currency_name: str,
    exchange_rate: float,
) -> str:
    """
    Insert or update a record.
    Returns: 'inserted' | 'updated' | 'skipped'
    """
    key = (str(exchange_date), currency_code)
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    if key in records:
        if UPSERT_RULE == "UPDATE":
            records[key]["exchange_rate"] = str(exchange_rate)
            records[key]["updated_at"] = now
            return "updated"
        else:  # SKIP
            return "skipped"
    else:
        records[key] = {
            "currency_name": currency_name,
            "exchange_rate": str(exchange_rate),
            "currency_code": currency_code,
            "exchange_date": str(exchange_date),
            "updated_at": now,
        }
        return "inserted"


# Main ETL pipeline
def run_etl() -> None:
    logger.info("=" * 60)
    logger.info("NBU Exchange Rate ETL — starting")
    logger.info(f"  Start date : {START_DATE}")
    logger.info(f"  Currencies : {CURRENCIES + ['UAH']}")
    logger.info(f"  Upsert rule: {UPSERT_RULE}")
    logger.info("=" * 60)

    today = date.today()

    # 1. Load existing data
    records = load_db()

    # 2. Seed UAH for every date (rate to itself = 1)
    #    We do this before the gap check so UAH never triggers a fetch.
    uah_inserted = 0
    current = START_DATE
    while current <= today:
        action = upsert_record(records, current, "UAH", "Ukrainian Hryvnia", 1.0)
        if action == "inserted":
            uah_inserted += 1
        current += timedelta(days=1)
    if uah_inserted:
        logger.info(f"Seeded {uah_inserted} UAH records (rate=1)")

    # 3. Detect missing dates for fetched currencies
    missing_dates = find_missing_dates(records, CURRENCIES, START_DATE, today)

    if not missing_dates:
        logger.info("No missing dates detected — database is up to date.")
        save_db(records)
        return

    # 4. Fetch & store missing dates
    stats = {"inserted": 0, "updated": 0, "skipped": 0, "failed": 0}

    for d in missing_dates:
        api_data = fetch_rates(d)

        if api_data is None:
            # Persistent failure — alert already logged, mark as failed
            stats["failed"] += 1
            continue

        if len(api_data) == 0:
            # Non-business day or genuinely empty — skip without error
            # We still need to write placeholder so the date isn't retried endlessly.
            # Strategy: mark date as attempted by inserting with rate=0.0
            # (business requirement can differ — here we keep the DB clean and skip)
            logger.info(f"[{d}] No rates returned (non-business day). Skipping.")
            continue

        # Build a lookup from the API response
        rate_lookup = {item["cc"]: item for item in api_data}

        for code in CURRENCIES:
            if code not in rate_lookup:
                logger.warning(f"[{d}] Currency {code} not found in API response")
                continue
            item = rate_lookup[code]
            action = upsert_record(
                records,
                d,
                code,
                item.get("txt", code),
                float(item.get("rate", 0)),
            )
            stats[action] += 1

    logger.info(
        f"ETL summary — inserted: {stats['inserted']}, "
        f"updated: {stats['updated']}, "
        f"skipped: {stats['skipped']}, "
        f"failed dates: {stats['failed']}"
    )

    # 5. Persist
    save_db(records)
    logger.info("ETL complete.")


if __name__ == "__main__":
    run_etl()

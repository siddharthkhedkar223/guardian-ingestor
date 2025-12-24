"""
fetcher.py - HTTP fetch layer with retry and missing-record handling.

On 404: raises RecordNotFoundException if HANDLE_MISSING_RECORDS is True,
        otherwise raises requests.HTTPError (simulates pre-fix crash behaviour).
On 5xx / timeout: retries with exponential backoff up to MAX_RETRY_ATTEMPTS.
"""

import time
from typing import Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import (
    API_BASE_URL,
    RESOURCE_ENDPOINT,
    HANDLE_MISSING_RECORDS,
    MAX_RETRY_ATTEMPTS,
    RETRY_BACKOFF_SECONDS,
)
from logger import get_logger

log = get_logger("fetcher")


class RecordNotFoundException(Exception):
    """Raised when the upstream source returns 404 for a requested record ID."""
    def __init__(self, record_id: int, error_body: str):
        self.record_id = record_id
        self.error_body = error_body
        super().__init__(f"Record ID={record_id} not found. Error body: '{error_body}'")


class TransientFetchError(Exception):
    """Raised for retryable errors (5xx, timeouts) after exhausting retries."""
    pass


def _build_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRY_ATTEMPTS,
        backoff_factor=RETRY_BACKOFF_SECONDS,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


_SESSION: requests.Session = _build_session()


def fetch_record(record_id: int, chaos_deleted: bool = False) -> Optional[Dict[str, Any]]:
    """
    Fetch a single record from the API.

    If chaos_deleted is True, skip the real HTTP call and simulate a 404.
    Returns parsed JSON on success, or raises on failure.
    """
    url = f"{API_BASE_URL}{RESOURCE_ENDPOINT}/{record_id}"

    if chaos_deleted:
        log.debug("Simulating 404 for Record ID=%d (chaos).", record_id)
        _handle_not_found(record_id, error_body="{}")
        return None

    log.debug("GET %s", url)

    try:
        response = _SESSION.get(url, timeout=10)
    except requests.exceptions.Timeout:
        raise TransientFetchError(f"Timeout fetching Record ID={record_id}")
    except requests.exceptions.ConnectionError as exc:
        raise TransientFetchError(f"Connection error fetching Record ID={record_id}: {exc}")

    if response.status_code == 404:
        error_body = response.text
        _handle_not_found(record_id, error_body)
        return None

    if not response.ok:
        raise TransientFetchError(
            f"HTTP {response.status_code} for Record ID={record_id}: {response.text[:200]}"
        )

    payload = response.json()
    log.info("Fetched Record ID=%d - title: '%s'", record_id, payload.get("title", "")[:60])
    return payload


def _handle_not_found(record_id: int, error_body: str) -> None:
    if HANDLE_MISSING_RECORDS:
        log.warning("Record ID=%d returned 404. Error body: %s", record_id, error_body or "{}")
        raise RecordNotFoundException(record_id=record_id, error_body=error_body)
    else:
        log.error(
            "Record ID=%d returned 404 and HANDLE_MISSING_RECORDS=False. Raising.",
            record_id,
        )
        raise requests.HTTPError(f"404 Not Found for Record ID={record_id}")

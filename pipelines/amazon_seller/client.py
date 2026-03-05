"""
Amazon SP-API client for Seller Central data.

Uses the Reports API v2021-06-30 for bulk order data (much faster than per-order API).
"""

import csv
import gzip
import io
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any

import requests

logger = logging.getLogger(__name__)

SP_API_BASE = "https://sellingpartnerapi-na.amazon.com"
LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
MARKETPLACE_US = "ATVPDKIKX0DER"

REPORTS_PATH = "/reports/2021-06-30"


class SPAPIError(Exception):
    """Base exception for SP-API errors."""


class SPAPIClient:
    """Amazon Selling Partner API client with LWA auth and Reports API."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        marketplace_id: str = MARKETPLACE_US,
        poll_interval: int = 30,
        poll_timeout_minutes: int = 30,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.marketplace_id = marketplace_id
        self.poll_interval = poll_interval
        self.poll_timeout_minutes = poll_timeout_minutes

        self._access_token: str | None = None
        self._token_expires_at: datetime | None = None
        self._session = requests.Session()

    # ── Auth ─────────────────────────────────────────────────────────

    def _refresh_access_token(self) -> str:
        """Get a fresh access token via LWA."""
        resp = self._session.post(
            LWA_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        expires_in = data.get("expires_in", 3600) - 300
        self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)
        logger.info("SP-API access token refreshed")
        return self._access_token

    def _get_token(self) -> str:
        if not self._access_token or (
            self._token_expires_at and datetime.now() >= self._token_expires_at
        ):
            return self._refresh_access_token()
        return self._access_token

    def _headers(self) -> dict[str, str]:
        return {
            "x-amz-access-token": self._get_token(),
            "Content-Type": "application/json",
            "User-Agent": "pickleball-data-platform/1.0",
        }

    # ── Request with retries ─────────────────────────────────────────

    def _request(
        self, method: str, path: str, params: dict | None = None, **kwargs: Any
    ) -> dict:
        """Make an SP-API request with rate-limit handling."""
        url = f"{SP_API_BASE}{path}"
        max_attempts = 10

        for attempt in range(1, max_attempts + 1):
            try:
                resp = self._session.request(
                    method, url, headers=self._headers(), params=params, timeout=30, **kwargs
                )
            except requests.exceptions.ConnectionError as e:
                wait = min(30 * attempt, 120)
                logger.warning(f"Connection error (attempt {attempt}): {e}. Waiting {wait}s")
                time.sleep(wait)
                continue

            if resp.status_code == 429:
                wait = min(5 * (2 ** (attempt - 1)), 120)
                logger.warning(f"Rate limited (attempt {attempt}). Waiting {wait}s")
                time.sleep(wait)
                continue

            if resp.status_code == 403:
                self._access_token = None
                if attempt < max_attempts:
                    logger.warning(f"Got 403, refreshing token and retrying: {resp.text}")
                    continue

            if resp.status_code >= 500:
                wait = min(5 * attempt, 30)
                logger.warning(
                    f"Server error {resp.status_code} (attempt {attempt}). Waiting {wait}s"
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        raise SPAPIError(f"Failed after {max_attempts} attempts: {path}")

    # ── Reports API ──────────────────────────────────────────────────

    def create_report(
        self,
        report_type: str,
        start_date: str,
        end_date: str,
        report_options: dict | None = None,
    ) -> str:
        """Create a report and return its report ID."""
        payload = {
            "reportType": report_type,
            "marketplaceIds": [self.marketplace_id],
            "dataStartTime": start_date,
            "dataEndTime": end_date,
        }
        if report_options:
            payload["reportOptions"] = report_options

        logger.info(f"Creating {report_type} report: {start_date} → {end_date}")
        data = self._request("POST", f"{REPORTS_PATH}/reports", json=payload)
        report_id = data["reportId"]
        logger.info(f"Report created: {report_id}")
        return report_id

    def wait_for_report(self, report_id: str) -> str:
        """Poll until report completes. Returns reportDocumentId."""
        deadline = time.time() + self.poll_timeout_minutes * 60

        while time.time() < deadline:
            data = self._request("GET", f"{REPORTS_PATH}/reports/{report_id}")
            status = data.get("processingStatus")
            logger.info(f"Report {report_id}: {status}")

            if status == "DONE":
                doc_id = data.get("reportDocumentId")
                if not doc_id:
                    raise SPAPIError(f"Report done but no documentId: {data}")
                return doc_id

            if status in ("CANCELLED", "FATAL"):
                raise SPAPIError(f"Report {report_id} failed: {status}")

            time.sleep(self.poll_interval)

        raise SPAPIError(
            f"Report {report_id} timed out after {self.poll_timeout_minutes} minutes"
        )

    def download_report(self, document_id: str, fmt: str = "tsv") -> list[dict] | dict:
        """Get report document URL, download, and parse.

        Args:
            document_id: The report document ID.
            fmt: "tsv" for tab-separated (returns list[dict]),
                 "json" for JSON reports (returns parsed dict/list).
        """
        data = self._request("GET", f"{REPORTS_PATH}/documents/{document_id}")
        url = data["url"]
        compression = data.get("compressionAlgorithm")

        logger.info(f"Downloading report document {document_id}...")
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()

        content = resp.content
        if compression == "GZIP":
            content = gzip.decompress(content)

        if fmt == "json":
            return json.loads(content.decode("utf-8"))

        text = content.decode("utf-8-sig")  # utf-8-sig handles BOM
        reader = csv.DictReader(io.StringIO(text), delimiter="\t")
        rows = list(reader)
        logger.info(f"Downloaded {len(rows)} rows")
        return rows

    def fetch_report(
        self,
        report_type: str,
        start_date: str,
        end_date: str,
        report_options: dict | None = None,
        fmt: str = "tsv",
    ) -> list[dict] | dict:
        """Generic end-to-end: create → poll → download any report."""
        report_id = self.create_report(
            report_type=report_type,
            start_date=start_date,
            end_date=end_date,
            report_options=report_options,
        )
        document_id = self.wait_for_report(report_id)
        return self.download_report(document_id, fmt=fmt)

    def fetch_order_report(
        self,
        start_date: str,
        end_date: str,
    ) -> list[dict]:
        """End-to-end: create → poll → download order report. Returns rows."""
        return self.fetch_report(
            report_type="GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
            start_date=start_date,
            end_date=end_date,
        )

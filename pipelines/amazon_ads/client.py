"""
Amazon Ads API client — ported from prefect_pipelines.

Handles LWA OAuth refresh, tenacity retries, async report creation/polling/download.
"""

import gzip
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

API_BASE = "https://advertising-api.amazon.com"
TOKEN_URL = "https://api.amazon.com/auth/o2/token"
REPORTS_PATH = "/reporting/reports"
PROFILES_PATH = "/v2/profiles"


class AmazonAdsError(Exception):
    """Base exception for Amazon Ads API errors."""


class AuthenticationError(AmazonAdsError):
    """Raised when authentication fails."""


class ReportError(AmazonAdsError):
    """Raised when report creation/polling fails."""


class AmazonAdsClient:
    """Amazon Ads API client with OAuth refresh, retries, and report lifecycle."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        poll_interval: int = 120,
        poll_timeout_minutes: int = 45,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.poll_interval = poll_interval
        self.poll_timeout_minutes = poll_timeout_minutes

        self._access_token: str | None = None
        self._token_expires_at: datetime | None = None
        self._session = requests.Session()

        # Eagerly fetch token on init
        self._refresh_access_token()

    # ── Auth ─────────────────────────────────────────────────────────

    def _refresh_access_token(self) -> str:
        """Refresh the LWA access token using the refresh token."""
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        try:
            resp = self._session.post(
                TOKEN_URL,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=data,
                timeout=30,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                raise AuthenticationError(
                    "Refresh token expired or invalid. Re-authorize the app."
                ) from e
            raise AuthenticationError(f"Token refresh failed: {e}") from e

        token_data = resp.json()
        self._access_token = token_data["access_token"]
        # Expire 5 minutes early for safety
        expires_in = token_data.get("expires_in", 3600) - 300
        self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)
        logger.info("Access token refreshed successfully")
        return self._access_token

    def _get_valid_token(self) -> str:
        """Return a valid access token, refreshing if needed."""
        if not self._access_token or (
            self._token_expires_at and datetime.now() >= self._token_expires_at
        ):
            return self._refresh_access_token()
        return self._access_token

    def _headers(self, profile_id: str) -> dict[str, str]:
        """Build request headers for a given profile."""
        return {
            "Authorization": f"Bearer {self._get_valid_token()}",
            "Amazon-Advertising-API-ClientId": self.client_id,
            "Amazon-Advertising-API-Scope": profile_id,
            "Content-Type": "application/json",
            "User-Agent": "pickleball-data-platform/1.0",
        }

    # ── Low-level request with retries ───────────────────────────────

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=5, max=120),
        retry=retry_if_exception_type((requests.exceptions.RequestException, AmazonAdsError)),
    )
    def _request(
        self, method: str, url: str, profile_id: str, **kwargs: Any
    ) -> requests.Response:
        """Make an API request with retries and rate-limit handling."""
        headers = self._headers(profile_id)
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))

        resp = self._session.request(method, url, headers=headers, **kwargs)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            logger.warning(f"Rate limited (429). Waiting {retry_after}s")
            time.sleep(retry_after)
            raise AmazonAdsError("Rate limited: 429")

        if resp.status_code >= 500:
            logger.warning(f"Server error {resp.status_code}, retrying")
            raise AmazonAdsError(f"Server error: {resp.status_code}")

        if resp.status_code >= 400:
            logger.error(f"Client error {resp.status_code}: {resp.text}")
            resp.raise_for_status()

        return resp

    # ── Report lifecycle ─────────────────────────────────────────────

    def create_report(
        self,
        profile_id: str,
        ad_product: str,
        report_type_id: str,
        start_date: str,
        end_date: str,
        columns: list[str],
        group_by: list[str] | None = None,
    ) -> str:
        """Create an async report and return the report ID.

        Handles 425 (duplicate report) by extracting the existing report ID.
        """
        url = f"{API_BASE}{REPORTS_PATH}"
        if group_by is None:
            group_by = ["campaign"]

        payload = {
            "name": f"{report_type_id}_{start_date}_{end_date}",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": ad_product,
                "reportTypeId": report_type_id,
                "timeUnit": "DAILY",
                "format": "GZIP_JSON",
                "groupBy": group_by,
                "columns": columns,
            },
        }

        logger.info(
            f"Creating {report_type_id} report for profile {profile_id}: "
            f"{start_date}→{end_date}"
        )

        # First attempt — handle 425 (duplicate) without retries
        resp = self._session.request(
            "POST", url, headers=self._headers(profile_id), json=payload
        )

        if resp.status_code == 425:
            try:
                detail = resp.json().get("detail", "")
                if ":" in detail:
                    report_id = detail.split(":")[-1].strip()
                    logger.warning(f"Duplicate report (425). Reusing ID: {report_id}")
                    return report_id
            except Exception:
                pass

        if 200 <= resp.status_code < 300:
            report_id = resp.json()["reportId"]
            logger.info(f"Report created: {report_id}")
            return report_id

        # Fall back to retriable request for transient errors
        resp = self._request("POST", url, profile_id, json=payload)
        report_id = resp.json()["reportId"]
        logger.info(f"Report created (retry): {report_id}")
        return report_id

    def wait_for_report(self, profile_id: str, report_id: str) -> str:
        """Poll until report completes. Returns download URL."""
        url = f"{API_BASE}{REPORTS_PATH}/{report_id}"
        deadline = time.time() + self.poll_timeout_minutes * 60

        logger.info(f"Polling report {report_id}...")

        while time.time() < deadline:
            data = self._request("GET", url, profile_id).json()
            status = data.get("status")
            logger.info(f"Report {report_id}: {status}")

            if status == "COMPLETED":
                # v3 API uses "url", older uses "location"
                download_url = data.get("url") or data.get("location")
                if not download_url:
                    raise ReportError(f"Report completed but no download URL: {data}")
                return download_url

            if status == "FAILED":
                raise ReportError(f"Report {report_id} failed: {data}")

            # PENDING / IN_PROGRESS — wait with jitter
            jitter = time.time() % 10
            time.sleep(self.poll_interval + jitter)

        raise ReportError(
            f"Report {report_id} timed out after {self.poll_timeout_minutes} minutes"
        )

    def download_report(self, download_url: str) -> list[dict]:
        """Download and decompress a GZIP JSON report. Returns list of row dicts."""
        logger.info("Downloading report...")
        resp = requests.get(download_url, timeout=120)
        resp.raise_for_status()

        decompressed = gzip.decompress(resp.content)
        rows = json.loads(decompressed.decode("utf-8"))
        logger.info(f"Downloaded {len(rows)} rows")
        return rows

    # ── Convenience ──────────────────────────────────────────────────

    def fetch_report(
        self,
        profile_id: str,
        ad_product: str,
        report_type_id: str,
        start_date: str,
        end_date: str,
        columns: list[str],
        group_by: list[str] | None = None,
    ) -> list[dict]:
        """End-to-end: create → poll → download. Returns rows."""
        report_id = self.create_report(
            profile_id, ad_product, report_type_id, start_date, end_date, columns, group_by
        )
        download_url = self.wait_for_report(profile_id, report_id)
        return self.download_report(download_url)

    def get_profiles(self) -> list[dict]:
        """Fetch all advertising profiles for this account."""
        url = f"{API_BASE}{PROFILES_PATH}"
        # Use empty string for profile scope — profiles endpoint doesn't need it
        resp = self._request("GET", url, "")
        return resp.json()

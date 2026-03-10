"""Google Search Console API client with OAuth2 authentication."""

import logging
from urllib.parse import urlparse

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

_MAX_ROWS = 25000


class SearchConsoleClient:
    """Search Console API client supporting multiple sites."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        site_url: str = "",
        site_url_shop: str = "",
    ):
        self.site_urls = [u for u in [site_url, site_url_shop] if u]

        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
        )
        self._service = build("searchconsole", "v1", credentials=creds)

    def validate_access(self):
        """Verify access to all configured sites."""
        sites = self._service.sites().list().execute()
        available = [s.get("siteUrl") for s in sites.get("siteEntry", [])]

        for target in self.site_urls:
            if target not in available:
                raise RuntimeError(
                    f"Site {target} not found in Search Console. Available: {available}"
                )
            logger.info(f"Search Console access verified: {target}")

    @staticmethod
    def site_label(site_url: str) -> str:
        """Derive a short site label from a Search Console site URL."""
        if site_url.startswith("sc-domain:"):
            return site_url.split(":", 1)[1]
        parsed = urlparse(site_url)
        return parsed.netloc or site_url

    def query_performance(self, site_url: str, start_date: str, end_date: str) -> list[dict]:
        """Fetch query+page performance data with offset pagination."""
        all_rows = []
        start_row = 0

        while True:
            request_body = {
                "startDate": start_date,
                "endDate": end_date,
                "dimensions": ["date", "query", "page", "country", "device"],
                "rowLimit": _MAX_ROWS,
                "startRow": start_row,
            }
            response = self._service.searchanalytics().query(
                siteUrl=site_url, body=request_body
            ).execute()

            rows = response.get("rows", [])
            if not rows:
                break

            all_rows.extend(rows)
            logger.info(f"[{self.site_label(site_url)}] Fetched {len(rows)} rows (offset {start_row})")

            if len(rows) < _MAX_ROWS:
                break
            start_row += _MAX_ROWS

        return all_rows

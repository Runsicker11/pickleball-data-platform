"""PayPal Reporting API client with OAuth2 client credentials and pagination."""

import base64
import logging
from datetime import datetime, timedelta, timezone

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

TOKEN_URL = "https://api-m.paypal.com/v1/oauth2/token"
API_BASE = "https://api-m.paypal.com"


class PayPalError(Exception):
    """Base exception for PayPal API errors."""


class PayPalAuthError(PayPalError):
    """Authentication/authorization failure (401/403)."""


class PayPalClient:
    """PayPal Reporting API client with OAuth2 client credentials flow."""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token: str | None = None
        self._token_expires_at: datetime | None = None
        self._session = requests.Session()

        self._fetch_access_token()

    def _fetch_access_token(self) -> str:
        """Fetch a new access token via client credentials grant."""
        credentials = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()

        resp = self._session.post(
            TOKEN_URL,
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            data={"grant_type": "client_credentials"},
            timeout=30,
        )

        if resp.status_code in (401, 403):
            raise PayPalAuthError(
                f"Token fetch failed ({resp.status_code}): {resp.text}. "
                "Check PAYPAL_CLIENT_ID and PAYPAL_CLIENT_SECRET."
            )
        resp.raise_for_status()

        token_data = resp.json()
        self._access_token = token_data["access_token"]
        # PayPal tokens last ~9 hours; refresh 5 min early
        self._token_expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=token_data.get("expires_in", 32400) - 300
        )

        logger.info("PayPal access token fetched")
        return self._access_token

    def _get_valid_token(self) -> str:
        """Return a valid access token, refreshing if expired."""
        if not self._access_token or (
            self._token_expires_at
            and datetime.now(timezone.utc) >= self._token_expires_at
        ):
            return self._fetch_access_token()
        return self._access_token

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_valid_token()}",
            "Content-Type": "application/json",
        }

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=5, max=120),
        retry=retry_if_exception_type((requests.exceptions.RequestException, PayPalError)),
    )
    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make an authenticated request with retries."""
        kwargs.setdefault("timeout", 60)
        resp = self._session.request(method, url, headers=self._headers(), **kwargs)

        if resp.status_code == 401:
            logger.warning("401 received, refreshing token and retrying")
            self._fetch_access_token()
            resp = self._session.request(method, url, headers=self._headers(), **kwargs)

        if resp.status_code == 429:
            raise PayPalError("Rate limited (429)")

        if resp.status_code >= 400:
            raise PayPalError(
                f"PayPal API error {resp.status_code}: {resp.text[:500]}"
            )

        return resp

    def get_transactions(self, start_date: datetime, end_date: datetime):
        """Yield raw transaction dicts for the given date range.

        PayPal limits to 31-day windows and 500 results per page.
        This method handles both chunking and pagination automatically.
        """
        chunk_start = start_date

        while chunk_start < end_date:
            chunk_end = min(chunk_start + timedelta(days=31), end_date)

            # Format as ISO 8601 with timezone
            start_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%S+0000")
            end_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%S+0000")

            page = 1
            total_pages = 1

            while page <= total_pages:
                url = f"{API_BASE}/v1/reporting/transactions"
                resp = self._request(
                    "GET",
                    url,
                    params={
                        "start_date": start_str,
                        "end_date": end_str,
                        "page_size": 500,
                        "page": page,
                        "fields": "all",
                    },
                )
                data = resp.json()

                total_pages = data.get("total_pages", 1)
                transaction_details = data.get("transaction_details", [])

                logger.info(
                    f"PayPal transactions chunk {chunk_start.date()}–{chunk_end.date()} "
                    f"page {page}/{total_pages}: {len(transaction_details)} rows"
                )

                yield from transaction_details
                page += 1

            chunk_start = chunk_end

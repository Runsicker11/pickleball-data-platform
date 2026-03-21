"""QuickBooks Online API client with OAuth2 token refresh and pagination."""

import base64
import logging
from datetime import datetime, timedelta

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
API_BASE = "https://quickbooks.api.intuit.com/v3/company"
MINOR_VERSION = "73"


class QuickBooksError(Exception):
    """Base exception for QuickBooks API errors."""


class QuickBooksAuthError(QuickBooksError):
    """Authentication/authorization failure (401/403)."""


class QuickBooksClient:
    """QuickBooks Online API client with OAuth2 refresh token flow."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        realm_id: str,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.realm_id = realm_id
        self._access_token: str | None = None
        self._token_expires_at: datetime | None = None
        self._session = requests.Session()

        self._refresh_access_token()

    def _refresh_access_token(self) -> str:
        """Exchange refresh token for a new access token."""
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
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            },
            timeout=30,
        )

        if resp.status_code in (401, 403):
            raise QuickBooksAuthError(
                f"Token refresh failed ({resp.status_code}): {resp.text}. "
                "Re-run: python -m pipelines.quickbooks.auth"
            )
        resp.raise_for_status()

        token_data = resp.json()
        self._access_token = token_data["access_token"]
        self._token_expires_at = datetime.now() + timedelta(
            seconds=token_data.get("expires_in", 3600) - 300  # refresh 5 min early
        )

        # QBO returns a new refresh token on each refresh — update it
        new_refresh = token_data.get("refresh_token")
        if new_refresh:
            self.refresh_token = new_refresh

        logger.info("QuickBooks access token refreshed")
        return self._access_token

    def _get_valid_token(self) -> str:
        """Return a valid access token, refreshing if needed."""
        if not self._access_token or (
            self._token_expires_at and datetime.now() >= self._token_expires_at
        ):
            return self._refresh_access_token()
        return self._access_token

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_valid_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    @property
    def base_url(self) -> str:
        return f"{API_BASE}/{self.realm_id}"

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=5, max=120),
        retry=retry_if_exception_type((requests.exceptions.RequestException, QuickBooksError)),
    )
    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make an authenticated request with retries."""
        kwargs.setdefault("timeout", 60)
        resp = self._session.request(method, url, headers=self._headers(), **kwargs)

        if resp.status_code == 401:
            logger.warning("401 received, refreshing token and retrying")
            self._refresh_access_token()
            resp = self._session.request(method, url, headers=self._headers(), **kwargs)

        if resp.status_code == 429:
            raise QuickBooksError("Rate limited (429)")

        if resp.status_code >= 400:
            raise QuickBooksError(
                f"QuickBooks API error {resp.status_code}: {resp.text[:500]}"
            )

        return resp

    def query(self, entity: str, where: str = "", order_by: str = "") -> list[dict]:
        """Execute a QBO query with automatic pagination.

        Args:
            entity: Entity name (e.g. "Invoice", "Customer", "Payment")
            where: Optional WHERE clause (e.g. "TxnDate >= '2024-01-01'")
            order_by: Optional ORDER BY clause (e.g. "TxnDate DESC")

        Returns:
            List of entity dicts from the API.
        """
        base_query = f"SELECT * FROM {entity}"
        if where:
            base_query += f" WHERE {where}"
        if order_by:
            base_query += f" ORDERBY {order_by}"

        all_results = []
        start_position = 1
        max_results = 1000

        while True:
            paginated_query = f"{base_query} STARTPOSITION {start_position} MAXRESULTS {max_results}"
            url = f"{self.base_url}/query"
            resp = self._request(
                "GET",
                url,
                params={"query": paginated_query, "minorversion": MINOR_VERSION},
            )
            data = resp.json()

            query_response = data.get("QueryResponse", {})
            entities = query_response.get(entity, [])
            all_results.extend(entities)

            total_count = query_response.get("totalCount", len(entities))
            logger.info(
                f"Query {entity}: fetched {len(entities)} (total so far: {len(all_results)}/{total_count})"
            )

            if len(entities) < max_results:
                break
            start_position += max_results

        return all_results

    def get_report(self, report_name: str, params: dict | None = None) -> dict:
        """Fetch a QBO report (e.g. ProfitAndLoss, BalanceSheet).

        Args:
            report_name: Report name (e.g. "ProfitAndLoss")
            params: Report parameters (e.g. date_macro, start_date, end_date)

        Returns:
            Full report response dict.
        """
        url = f"{self.base_url}/reports/{report_name}"
        query_params = {"minorversion": MINOR_VERSION}
        if params:
            query_params.update(params)
        resp = self._request("GET", url, params=query_params)
        return resp.json()

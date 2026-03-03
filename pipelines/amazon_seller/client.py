"""
Amazon SP-API client for Seller Central data (orders, inventory, catalog).

Uses the Orders API v0 and Reports API for bulk order data.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Any

import requests

logger = logging.getLogger(__name__)

SP_API_BASE = "https://sellingpartnerapi-na.amazon.com"
LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
MARKETPLACE_US = "ATVPDKIKX0DER"


class SPAPIError(Exception):
    """Base exception for SP-API errors."""


class SPAPIClient:
    """Amazon Selling Partner API client with LWA auth and pagination."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        marketplace_id: str = MARKETPLACE_US,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.marketplace_id = marketplace_id

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
        max_attempts = 5

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
                retry_after = int(resp.headers.get("x-amzn-RateLimit-Limit", 2))
                wait = max(1.0 / retry_after if retry_after else 2, 2)
                logger.warning(f"Rate limited (attempt {attempt}). Waiting {wait}s")
                time.sleep(wait)
                continue

            if resp.status_code == 403:
                # Token may have expired
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

    # ── Orders API ───────────────────────────────────────────────────

    def get_orders(
        self,
        created_after: str,
        created_before: str | None = None,
        order_statuses: list[str] | None = None,
    ) -> list[dict]:
        """Fetch orders with automatic pagination.

        Args:
            created_after: ISO 8601 datetime string
            created_before: Optional end datetime
            order_statuses: Optional filter, e.g. ["Shipped", "Unshipped"]

        Returns:
            List of order dicts
        """
        params: dict[str, Any] = {
            "MarketplaceIds": self.marketplace_id,
            "CreatedAfter": created_after,
        }
        if created_before:
            params["CreatedBefore"] = created_before
        if order_statuses:
            params["OrderStatuses"] = ",".join(order_statuses)

        all_orders = []
        next_token = None

        while True:
            if next_token:
                data = self._request("GET", "/orders/v0/orders", params={"NextToken": next_token})
            else:
                data = self._request("GET", "/orders/v0/orders", params=params)

            payload = data.get("payload", {})
            orders = payload.get("Orders", [])
            all_orders.extend(orders)
            logger.info(f"Fetched {len(orders)} orders (total: {len(all_orders)})")

            next_token = payload.get("NextToken")
            if not next_token:
                break

            # Small delay between pages to respect rate limits
            time.sleep(0.5)

        return all_orders

    def get_order_items(self, order_id: str) -> list[dict]:
        """Fetch line items for a specific order."""
        all_items = []
        next_token = None

        while True:
            params = {}
            if next_token:
                params["NextToken"] = next_token

            data = self._request(
                "GET", f"/orders/v0/orders/{order_id}/orderItems", params=params
            )

            payload = data.get("payload", {})
            items = payload.get("OrderItems", [])

            for item in items:
                item["AmazonOrderId"] = order_id

            all_items.extend(items)

            next_token = payload.get("NextToken")
            if not next_token:
                break

            time.sleep(0.3)

        return all_items

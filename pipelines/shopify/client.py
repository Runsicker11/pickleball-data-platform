"""Shopify REST + GraphQL API client with auth and pagination."""

import logging
import time

import requests

logger = logging.getLogger(__name__)


class ShopifyClient:
    """Shopify API client supporting static tokens and Client Credentials Grant."""

    def __init__(
        self,
        shop_domain: str,
        client_id: str,
        client_secret: str,
        access_token: str = "",
        api_version: str = "2025-01",
    ):
        self.shop_domain = shop_domain
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_version = api_version
        self._session = requests.Session()

        if access_token:
            self._access_token = access_token
            logger.info("Using static Shopify access token")
        else:
            self._access_token = self._client_credentials_grant()

        self._session.headers.update({
            "X-Shopify-Access-Token": self._access_token,
            "Content-Type": "application/json",
        })

    def _client_credentials_grant(self) -> str:
        """Obtain token via Client Credentials Grant."""
        url = f"https://{self.shop_domain}/admin/oauth/access_token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
        resp = self._session.post(url, data=payload, timeout=30)
        resp.raise_for_status()
        token = resp.json()["access_token"]
        logger.info("Shopify token acquired via Client Credentials Grant")
        return token

    @property
    def base_url(self) -> str:
        return f"https://{self.shop_domain}/admin/api/{self.api_version}"

    @property
    def graphql_url(self) -> str:
        return f"{self.base_url}/graphql.json"

    def get_paginated(self, endpoint: str, params: dict, key: str) -> list[dict]:
        """Fetch all pages of a REST endpoint using Link header pagination.

        Args:
            endpoint: REST endpoint path (e.g. "/orders.json")
            params: Query parameters for first request
            key: JSON key containing the data array (e.g. "orders")
        """
        from .helpers import parse_link_header

        url = f"{self.base_url}{endpoint}"
        all_data = []
        page = 0

        while url:
            page += 1
            resp = self._session.get(
                url, params=params if page == 1 else None, timeout=60
            )
            if resp.status_code == 429:
                retry_after = int(float(resp.headers.get("Retry-After", 10)))
                logger.warning(f"Page {page}: rate limited (429), waiting {retry_after}s")
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            items = resp.json().get(key, [])
            all_data.extend(items)
            logger.info(f"Page {page}: fetched {len(items)} {key} (total: {len(all_data)})")
            url = parse_link_header(resp.headers.get("Link"))

        return all_data

    def graphql(self, query: str, variables: dict | None = None) -> dict:
        """Execute a GraphQL query."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        resp = self._session.post(self.graphql_url, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]

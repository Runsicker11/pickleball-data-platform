"""Klaviyo API v3 client — thin wrapper over requests with cursor pagination."""

import logging
import time
from collections.abc import Iterator
from typing import Any

import requests

logger = logging.getLogger(__name__)

_RATE_LIMIT_SLEEP = 10  # seconds to wait on 429


class KlaviyoClient:
    BASE_URL = "https://a.klaviyo.com/api"
    REVISION = "2024-10-15"

    def __init__(self, api_key: str) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Klaviyo-API-Key {api_key}",
                "revision": self.REVISION,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def get(self, endpoint: str, params: dict | None = None) -> dict:
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        for attempt in range(3):
            resp = self.session.get(url, params=params)
            if resp.status_code == 429:
                logger.warning("Rate limited — sleeping %ds", _RATE_LIMIT_SLEEP)
                time.sleep(_RATE_LIMIT_SLEEP)
                continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()
        return {}

    def post(self, endpoint: str, payload: dict) -> dict:
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        for attempt in range(3):
            resp = self.session.post(url, json=payload)
            if resp.status_code == 429:
                logger.warning("Rate limited — sleeping %ds", _RATE_LIMIT_SLEEP)
                time.sleep(_RATE_LIMIT_SLEEP)
                continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()
        return {}

    def paginate(self, endpoint: str, params: dict | None = None) -> Iterator[dict]:
        """Yield individual data items across all cursor-paginated pages."""
        params = dict(params or {})
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        while url:
            for attempt in range(3):
                resp = self.session.get(url, params=params)
                if resp.status_code == 429:
                    logger.warning("Rate limited — sleeping %ds", _RATE_LIMIT_SLEEP)
                    time.sleep(_RATE_LIMIT_SLEEP)
                    continue
                resp.raise_for_status()
                break

            data: dict[str, Any] = resp.json()
            for item in data.get("data", []):
                yield item

            # Cursor pagination: next URL is fully qualified
            url = (data.get("links") or {}).get("next")
            params = {}  # next URL already contains all params

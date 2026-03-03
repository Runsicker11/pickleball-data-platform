"""
dlt source for Amazon Seller Central — order report via Reports API.

Uses GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL which returns
one row per order line item with all order + item fields combined.
"""

import logging
from datetime import datetime, timedelta, timezone

import dlt

from .client import SPAPIClient

logger = logging.getLogger(__name__)


def _normalize_row(row: dict) -> dict:
    """Normalize a flat-file report row for loading.

    Converts hyphenated column names to underscores and coerces types.
    """
    flat = {}
    for key, value in row.items():
        # Convert header names: "amazon-order-id" → "amazon_order_id"
        col = key.replace("-", "_")
        # Treat empty strings as None
        flat[col] = value if value != "" else None
    flat["_loaded_at"] = datetime.now(timezone.utc).isoformat()
    return flat


@dlt.source(name="amazon_seller")
def amazon_seller_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    marketplace_id: str = "ATVPDKIKX0DER",
    days_back: int = 30,
):
    """dlt source for Amazon Seller Central orders data via Reports API."""

    client = SPAPIClient(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        marketplace_id=marketplace_id,
    )

    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    start_date = (
        datetime.now(timezone.utc) - timedelta(days=days_back)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    @dlt.resource(
        name="seller_orders",
        write_disposition="merge",
        merge_key=["amazon_order_id", "sku"],
        primary_key=["amazon_order_id", "sku"],
    )
    def seller_orders():
        logger.info(f"Requesting order report: {start_date} → {end_date}")
        rows = client.fetch_order_report(start_date=start_date, end_date=end_date)
        logger.info(f"Got {len(rows)} order line items, normalizing...")
        for row in rows:
            yield _normalize_row(row)

    yield seller_orders

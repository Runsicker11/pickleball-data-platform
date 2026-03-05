"""
dlt source for Amazon Seller Central — Reports API.

Resources:
  - seller_orders: merged from by-order-date + by-last-update reports
  - seller_fba_shipments: FBA fulfilled shipments (buyer, carrier, tracking)
  - seller_traffic: Sales & Traffic by ASIN (sessions, page views, buy box)
  - seller_fba_fees: FBA fee estimates per SKU
"""

import logging
import re
from datetime import datetime, timedelta, timezone

import dlt
import requests

from .client import SPAPIClient, SPAPIError

logger = logging.getLogger(__name__)


def _camel_to_snake(name: str) -> str:
    """Convert camelCase to snake_case."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


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


def _flatten_traffic_row(record: dict, report_date: str) -> dict:
    """Flatten nested salesAndTrafficByAsin JSON into a flat dict."""
    flat = {
        "child_asin": record.get("childAsin"),
        "parent_asin": record.get("parentAsin"),
        "report_date": report_date,
    }
    # Flatten salesByAsin (nested money objects)
    sales = record.get("salesByAsin", {})
    flat["units_ordered"] = sales.get("unitsOrdered", 0)
    flat["units_ordered_b2b"] = sales.get("unitsOrderedB2B", 0)
    flat["total_order_items"] = sales.get("totalOrderItems", 0)
    ops = sales.get("orderedProductSales", {})
    flat["ordered_product_sales_amount"] = ops.get("amount", 0)
    flat["ordered_product_sales_currency"] = ops.get("currencyCode")
    # Flatten trafficByAsin (already flat keys)
    traffic = record.get("trafficByAsin", {})
    for key, val in traffic.items():
        flat[_camel_to_snake(key)] = val
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
    """dlt source for Amazon Seller Central data via Reports API."""

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

    # ── Resource 1: Orders (merged from 2 report types) ──────────

    @dlt.resource(
        name="seller_orders",
        write_disposition="merge",
        merge_key=["amazon_order_id", "sku"],
        primary_key=["amazon_order_id", "sku"],
    )
    def seller_orders():
        for report_type in [
            "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
            "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL",
        ]:
            try:
                rows = client.fetch_report(report_type, start_date, end_date)
            except (requests.exceptions.HTTPError, SPAPIError) as e:
                logger.warning(f"Skipping {report_type}: {e}")
                continue
            logger.info(f"{report_type}: {len(rows)} rows")
            for row in rows:
                yield _normalize_row(row)

    # ── Resource 2: FBA Shipments ─────────────────────────────────

    @dlt.resource(
        name="seller_fba_shipments",
        write_disposition="merge",
        merge_key=["amazon_order_id", "sku", "shipment_id", "shipment_item_id"],
        primary_key=["amazon_order_id", "sku", "shipment_id", "shipment_item_id"],
    )
    def seller_fba_shipments():
        try:
            rows = client.fetch_report(
                "GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL", start_date, end_date
            )
        except (requests.exceptions.HTTPError, SPAPIError) as e:
            logger.warning(f"Skipping FBA shipments: {e}")
            return
        logger.info(f"FBA shipments: {len(rows)} rows")
        for row in rows:
            yield _normalize_row(row)

    # ── Resource 3: Sales & Traffic (JSON) ────────────────────────

    @dlt.resource(
        name="seller_traffic",
        write_disposition="merge",
        merge_key=["child_asin", "report_date"],
        primary_key=["child_asin", "report_date"],
    )
    def seller_traffic():
        try:
            data = client.fetch_report(
                "GET_SALES_AND_TRAFFIC_REPORT",
                start_date,
                end_date,
                report_options={"asinGranularity": "CHILD"},
                fmt="json",
            )
        except (requests.exceptions.HTTPError, SPAPIError) as e:
            logger.warning(f"Skipping Sales & Traffic: {e}")
            return
        report_date = data.get("reportSpecification", {}).get("dataStartTime")
        records = data.get("salesAndTrafficByAsin", [])
        logger.info(f"Sales & Traffic: {len(records)} ASINs for {report_date}")
        for record in records:
            yield _flatten_traffic_row(record, report_date)

    # ── Resource 4: FBA Fees ──────────────────────────────────────

    @dlt.resource(
        name="seller_fba_fees",
        write_disposition="merge",
        merge_key=["sku", "asin"],
        primary_key=["sku", "asin"],
    )
    def seller_fba_fees():
        try:
            rows = client.fetch_report(
                "GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA", start_date, end_date
            )
        except (requests.exceptions.HTTPError, SPAPIError) as e:
            logger.warning(f"Skipping FBA fees: {e}")
            return
        logger.info(f"FBA fees: {len(rows)} SKUs")
        for row in rows:
            yield _normalize_row(row)

    yield seller_orders
    yield seller_fba_shipments
    yield seller_traffic
    yield seller_fba_fees

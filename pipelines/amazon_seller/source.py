"""
dlt source for Amazon Seller Central — orders and order items.
"""

import logging
from datetime import datetime, timedelta, timezone

import dlt

from .client import SPAPIClient

logger = logging.getLogger(__name__)


def _flatten_money(obj: dict | None, prefix: str = "") -> dict:
    """Flatten Amazon money objects like {"CurrencyCode": "USD", "Amount": "14.99"}."""
    if not obj:
        return {}
    result = {}
    if prefix:
        prefix = f"{prefix}_"
    result[f"{prefix}currency"] = obj.get("CurrencyCode")
    amount = obj.get("Amount")
    result[f"{prefix}amount"] = float(amount) if amount else None
    return result


def _flatten_order(order: dict) -> dict:
    """Flatten a single order dict for loading."""
    flat = {
        "amazon_order_id": order.get("AmazonOrderId"),
        "purchase_date": order.get("PurchaseDate"),
        "last_update_date": order.get("LastUpdateDate"),
        "order_status": order.get("OrderStatus"),
        "fulfillment_channel": order.get("FulfillmentChannel"),
        "sales_channel": order.get("SalesChannel"),
        "ship_service_level": order.get("ShipServiceLevel"),
        "number_of_items_shipped": order.get("NumberOfItemsShipped"),
        "number_of_items_unshipped": order.get("NumberOfItemsUnshipped"),
        "order_type": order.get("OrderType"),
        "earliest_ship_date": order.get("EarliestShipDate"),
        "latest_ship_date": order.get("LatestShipDate"),
        "is_business_order": order.get("IsBusinessOrder"),
        "marketplace_id": order.get("MarketplaceId"),
    }
    flat.update(_flatten_money(order.get("OrderTotal"), "order_total"))

    # Shipping address (may be restricted)
    addr = order.get("ShippingAddress", {})
    if addr:
        flat["ship_city"] = addr.get("City")
        flat["ship_state"] = addr.get("StateOrRegion")
        flat["ship_postal_code"] = addr.get("PostalCode")
        flat["ship_country"] = addr.get("CountryCode")

    flat["_loaded_at"] = datetime.now(timezone.utc).isoformat()
    return flat


def _flatten_order_item(item: dict) -> dict:
    """Flatten a single order item dict for loading."""
    flat = {
        "amazon_order_id": item.get("AmazonOrderId"),
        "order_item_id": item.get("OrderItemId"),
        "asin": item.get("ASIN"),
        "seller_sku": item.get("SellerSKU"),
        "title": item.get("Title"),
        "quantity_ordered": item.get("QuantityOrdered"),
        "quantity_shipped": item.get("QuantityShipped"),
        "is_gift": item.get("IsGift"),
    }
    flat.update(_flatten_money(item.get("ItemPrice"), "item_price"))
    flat.update(_flatten_money(item.get("ItemTax"), "item_tax"))
    flat.update(_flatten_money(item.get("PromotionDiscount"), "promotion_discount"))
    flat.update(_flatten_money(item.get("ShippingPrice"), "shipping_price"))
    flat.update(_flatten_money(item.get("ShippingTax"), "shipping_tax"))

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
    """dlt source for Amazon Seller Central orders data."""

    client = SPAPIClient(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        marketplace_id=marketplace_id,
    )

    created_after = (
        datetime.now(timezone.utc) - timedelta(days=days_back)
    ).isoformat()

    @dlt.resource(
        name="seller_orders",
        write_disposition="merge",
        merge_key="amazon_order_id",
        primary_key="amazon_order_id",
    )
    def seller_orders():
        logger.info(f"Fetching orders since {created_after}")
        orders = client.get_orders(created_after=created_after)
        logger.info(f"Got {len(orders)} orders, flattening...")
        for order in orders:
            yield _flatten_order(order)

    @dlt.resource(
        name="seller_order_items",
        write_disposition="merge",
        merge_key="order_item_id",
        primary_key="order_item_id",
    )
    def seller_order_items():
        logger.info(f"Fetching orders for line items since {created_after}")
        orders = client.get_orders(created_after=created_after)
        logger.info(f"Got {len(orders)} orders, fetching items for each...")
        for i, order in enumerate(orders):
            order_id = order.get("AmazonOrderId")
            if not order_id:
                continue
            items = client.get_order_items(order_id)
            for item in items:
                yield _flatten_order_item(item)
            if (i + 1) % 50 == 0:
                logger.info(f"Processed items for {i + 1}/{len(orders)} orders")

    yield seller_orders
    yield seller_order_items

"""Shopify dlt source — orders, line_items, products, variants, customers, sessions."""

import logging
from datetime import date, datetime, timedelta, timezone

import dlt

from .client import ShopifyClient
from .helpers import gid_to_int, now_utc_str, parse_utms, safe_float

logger = logging.getLogger(__name__)

SESSIONS_QUERY_TEMPLATE = """
FROM sessions
  SHOW sessions, product_views, add_to_carts, checkouts, orders
  GROUP BY day, referrer_source
  SINCE -{days}d UNTIL today
"""

SESSIONS_GQL = """
query ($query: String!) {
  shopifyqlQuery(query: $query) {
    __typename
    ... on TableResponse {
      tableData {
        columns { name dataType }
        rowData
      }
    }
    parseErrors { code message range { start { line character } end { line character } } }
  }
}
"""

PRODUCTS_QUERY = """
query ($cursor: String) {
  products(first: 50, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      title
      handle
      productType
      vendor
      status
      tags
      createdAt
      updatedAt
      variants(first: 100) {
        nodes {
          id
          title
          sku
          price
          compareAtPrice
          inventoryQuantity
        }
      }
    }
  }
}
"""


@dlt.source(name="shopify")
def shopify_source(
    shop_domain: str = dlt.secrets.value,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    access_token: str = "",
    api_version: str = "2025-01",
    days_back: int = 3,
):
    """dlt source yielding 6 Shopify resources."""
    client = ShopifyClient(
        shop_domain=shop_domain,
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        api_version=api_version,
    )
    since_date = date.today() - timedelta(days=days_back)

    yield _orders_resource(client, since_date)
    yield _order_line_items_resource(client, since_date)
    yield _products_resource(client)
    yield _product_variants_resource(client)
    yield _customers_resource(client)
    yield _sessions_resource(client, days_back)


def _orders_resource(client: ShopifyClient, since_date: date):
    @dlt.resource(
        name="orders",
        write_disposition="merge",
        merge_key="order_id",
        primary_key="order_id",
    )
    def orders():
        now_str = now_utc_str()
        since_iso = datetime.combine(
            since_date, datetime.min.time(), tzinfo=timezone.utc
        ).isoformat()

        raw_orders = client.get_paginated(
            "/orders.json",
            params={
                "status": "any",
                "created_at_min": since_iso,
                "limit": 250,
                "fields": (
                    "id,order_number,created_at,updated_at,financial_status,"
                    "fulfillment_status,total_price,subtotal_price,total_tax,"
                    "total_shipping_price_set,total_discounts,currency,customer,"
                    "landing_site,referring_site,source_name,cancelled_at,"
                    "cancel_reason,tags,note,line_items"
                ),
            },
            key="orders",
        )

        for o in raw_orders:
            order_date_val = o["created_at"][:10]
            utms = parse_utms(o.get("landing_site"))

            shipping_set = o.get("total_shipping_price_set")
            total_shipping = 0.0
            if shipping_set and shipping_set.get("shop_money"):
                total_shipping = float(shipping_set["shop_money"].get("amount", 0))

            customer = o.get("customer") or {}

            yield {
                "order_id": o["id"],
                "order_number": o.get("order_number"),
                "created_at": o["created_at"],
                "updated_at": o.get("updated_at"),
                "financial_status": o.get("financial_status"),
                "fulfillment_status": o.get("fulfillment_status"),
                "total_price": float(o.get("total_price", 0)),
                "subtotal_price": float(o.get("subtotal_price", 0)),
                "total_tax": float(o.get("total_tax", 0)),
                "total_shipping": total_shipping,
                "total_discounts": float(o.get("total_discounts", 0)),
                "currency": o.get("currency"),
                "customer_id": customer.get("id"),
                "customer_email": customer.get("email"),
                "landing_site": o.get("landing_site"),
                "referring_site": o.get("referring_site"),
                "source_name": o.get("source_name"),
                **utms,
                "cancelled_at": o.get("cancelled_at"),
                "cancel_reason": o.get("cancel_reason"),
                "tags": o.get("tags"),
                "note": o.get("note"),
                "order_date": order_date_val,
                "ingested_at": now_str,
            }

    return orders


def _order_line_items_resource(client: ShopifyClient, since_date: date):
    @dlt.resource(
        name="order_line_items",
        write_disposition="merge",
        merge_key="line_item_id",
        primary_key="line_item_id",
    )
    def order_line_items():
        now_str = now_utc_str()
        since_iso = datetime.combine(
            since_date, datetime.min.time(), tzinfo=timezone.utc
        ).isoformat()

        raw_orders = client.get_paginated(
            "/orders.json",
            params={
                "status": "any",
                "created_at_min": since_iso,
                "limit": 250,
                "fields": "id,created_at,line_items",
            },
            key="orders",
        )

        for o in raw_orders:
            order_date_val = o["created_at"][:10]
            for li in o.get("line_items", []):
                yield {
                    "line_item_id": li["id"],
                    "order_id": o["id"],
                    "product_id": li.get("product_id"),
                    "variant_id": li.get("variant_id"),
                    "title": li.get("title"),
                    "variant_title": li.get("variant_title"),
                    "sku": li.get("sku"),
                    "quantity": li.get("quantity"),
                    "price": float(li.get("price", 0)),
                    "total_discount": float(li.get("total_discount", 0)),
                    "order_date": order_date_val,
                    "ingested_at": now_str,
                }

    return order_line_items


def _products_resource(client: ShopifyClient):
    @dlt.resource(
        name="products",
        write_disposition="replace",
    )
    def products():
        now_str = now_utc_str()
        cursor = None

        while True:
            data = client.graphql(PRODUCTS_QUERY, {"cursor": cursor})
            products_data = data["products"]

            for p in products_data["nodes"]:
                yield {
                    "product_id": gid_to_int(p["id"]),
                    "title": p["title"],
                    "handle": p["handle"],
                    "product_type": p.get("productType"),
                    "vendor": p.get("vendor"),
                    "status": p.get("status"),
                    "tags": ", ".join(p.get("tags", [])),
                    "created_at": p.get("createdAt"),
                    "updated_at": p.get("updatedAt"),
                    "ingested_at": now_str,
                }

            page_info = products_data["pageInfo"]
            if page_info["hasNextPage"]:
                cursor = page_info["endCursor"]
            else:
                break

    return products


def _product_variants_resource(client: ShopifyClient):
    @dlt.resource(
        name="product_variants",
        write_disposition="replace",
    )
    def product_variants():
        now_str = now_utc_str()
        cursor = None

        while True:
            data = client.graphql(PRODUCTS_QUERY, {"cursor": cursor})
            products_data = data["products"]

            for p in products_data["nodes"]:
                product_id = gid_to_int(p["id"])
                for v in p.get("variants", {}).get("nodes", []):
                    yield {
                        "variant_id": gid_to_int(v["id"]),
                        "product_id": product_id,
                        "title": v.get("title"),
                        "sku": v.get("sku"),
                        "price": safe_float(v.get("price")),
                        "compare_at_price": safe_float(v.get("compareAtPrice")),
                        "inventory_quantity": v.get("inventoryQuantity"),
                        "weight": None,
                        "weight_unit": None,
                        "ingested_at": now_str,
                    }

            page_info = products_data["pageInfo"]
            if page_info["hasNextPage"]:
                cursor = page_info["endCursor"]
            else:
                break

    return product_variants


def _customers_resource(client: ShopifyClient):
    @dlt.resource(
        name="customers",
        write_disposition="replace",
    )
    def customers():
        now_str = now_utc_str()
        raw = client.get_paginated(
            "/customers.json",
            params={
                "limit": 250,
                "fields": (
                    "id,email,first_name,last_name,orders_count,total_spent,"
                    "created_at,updated_at,state,accepts_marketing,"
                    "default_address,tags"
                ),
            },
            key="customers",
        )

        for c in raw:
            addr = c.get("default_address") or {}
            yield {
                "customer_id": c["id"],
                "email": c.get("email"),
                "first_name": c.get("first_name"),
                "last_name": c.get("last_name"),
                "orders_count": c.get("orders_count"),
                "total_spent": float(c.get("total_spent", 0)),
                "created_at": c.get("created_at"),
                "updated_at": c.get("updated_at"),
                "state": c.get("state"),
                "accepts_marketing": c.get("accepts_marketing", False),
                "city": addr.get("city"),
                "province": addr.get("province"),
                "country": addr.get("country"),
                "tags": c.get("tags"),
                "ingested_at": now_str,
            }

    return customers


def _sessions_resource(client: ShopifyClient, days_back: int):
    @dlt.resource(
        name="shopify_sessions",
        write_disposition="replace",
    )
    def sessions():
        now_str = now_utc_str()
        query = SESSIONS_QUERY_TEMPLATE.format(days=days_back)
        try:
            data = client.graphql(SESSIONS_GQL, {"query": query})
        except RuntimeError as e:
            if "shopifyqlQuery" in str(e) or "doesn't exist" in str(e):
                logger.warning("ShopifyQL not available on this Shopify plan — skipping sessions")
                return
            raise

        response = data["shopifyqlQuery"]
        if response["__typename"] != "TableResponse":
            parse_errors = response.get("parseErrors", [])
            raise RuntimeError(f"ShopifyQL query failed: {parse_errors}")

        table = response["tableData"]
        col_names = [c["name"] for c in table["columns"]]
        logger.info(f"ShopifyQL sessions: {len(table['rowData'])} rows, columns={col_names}")

        for row in table["rowData"]:
            record = dict(zip(col_names, row))
            yield {
                "report_date": record.get("day"),
                "referrer_source": record.get("referrer_source", ""),
                "sessions": int(record.get("sessions", 0)),
                "product_views": int(record.get("product_views", 0)),
                "add_to_carts": int(record.get("add_to_carts", 0)),
                "checkouts": int(record.get("checkouts", 0)),
                "orders": int(record.get("orders", 0)),
                "ingested_at": now_str,
            }

    return sessions

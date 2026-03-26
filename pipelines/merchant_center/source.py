"""dlt source for Merchant Center — 3 resources: products, product_statuses, shopping_ads_program."""

import logging
from datetime import datetime, timezone

import dlt

from .client import MerchantCenterClient

logger = logging.getLogger(__name__)


@dlt.source(name="merchant_center")
def merchant_center_source(
    merchant_id: str = dlt.secrets.value,
    sa_key_json: str = "",
):
    """dlt source yielding 3 Merchant Center resources."""
    client = MerchantCenterClient(merchant_id=merchant_id, sa_key_json=sa_key_json)
    client.validate_access()

    yield _products_resource(client)
    yield _product_statuses_resource(client)
    yield _shopping_ads_program_resource(client)


def _products_resource(client: MerchantCenterClient):
    @dlt.resource(name="products", write_disposition="replace")
    def products():
        ingested = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for item in client.list_products():
            price = item.get("price", {})
            yield {
                "product_id": item.get("id", ""),
                "title": item.get("title", ""),
                "description": item.get("description", ""),
                "link": item.get("link", ""),
                "image_link": item.get("imageLink", ""),
                "availability": item.get("availability", ""),
                "price_value": price.get("value", ""),
                "price_currency": price.get("currency", ""),
                "brand": item.get("brand", ""),
                "condition": item.get("condition", ""),
                "custom_label_0": item.get("customLabel0", ""),
                "custom_label_1": item.get("customLabel1", ""),
                "custom_label_2": item.get("customLabel2", ""),
                "custom_label_3": item.get("customLabel3", ""),
                "custom_label_4": item.get("customLabel4", ""),
                "product_types": ", ".join(item.get("productTypes", [])),
                "google_product_category": item.get("googleProductCategory", ""),
                "ingested_at": ingested,
            }

    return products


def _product_statuses_resource(client: MerchantCenterClient):
    @dlt.resource(name="product_statuses", write_disposition="replace")
    def product_statuses():
        ingested = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for item in client.list_product_statuses():
            issues = item.get("itemLevelIssues", [])
            disapproval_reasons = ", ".join(
                i.get("description", "") for i in issues if i.get("servability") == "disapproved"
            )
            # Overall approval status: approved if no disapproved issues
            has_disapproval = any(i.get("servability") == "disapproved" for i in issues)
            approval_status = "disapproved" if has_disapproval else "approved"

            for issue in issues:
                yield {
                    "product_id": item.get("productId", ""),
                    "title": item.get("title", ""),
                    "approval_status": approval_status,
                    "disapproval_reasons": disapproval_reasons,
                    "issue_code": issue.get("code", ""),
                    "issue_servability": issue.get("servability", ""),
                    "issue_resolution": issue.get("resolution", ""),
                    "issue_attribute": issue.get("attribute", ""),
                    "issue_destination": issue.get("destination", ""),
                    "issue_description": issue.get("description", ""),
                    "ingested_at": ingested,
                }

            # Always yield at least one row per product (even with no issues)
            if not issues:
                yield {
                    "product_id": item.get("productId", ""),
                    "title": item.get("title", ""),
                    "approval_status": approval_status,
                    "disapproval_reasons": "",
                    "issue_code": "",
                    "issue_servability": "",
                    "issue_resolution": "",
                    "issue_attribute": "",
                    "issue_destination": "",
                    "issue_description": "",
                    "ingested_at": ingested,
                }

    return product_statuses


def _shopping_ads_program_resource(client: MerchantCenterClient):
    @dlt.resource(name="shopping_ads_program", write_disposition="replace")
    def shopping_ads_program():
        ingested = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        data = client.get_shopping_ads_program()
        state = data.get("state", "")
        for region in data.get("regionStatuses", []):
            yield {
                "state": state,
                "region_code": region.get("regionCode", ""),
                "eligibility_status": region.get("eligibilityStatus", ""),
                "review_issues": ", ".join(region.get("reviewIssues", [])),
                "ingested_at": ingested,
            }
        if not data.get("regionStatuses"):
            yield {
                "state": state,
                "region_code": "",
                "eligibility_status": "",
                "review_issues": "",
                "ingested_at": ingested,
            }

    return shopping_ads_program

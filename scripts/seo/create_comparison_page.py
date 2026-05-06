"""Create the Tungsten vs Lead Tape comparison page as a draft in Shopify.

The page is created with `published: false` so it appears in Shopify admin
under Online Store -> Pages but is NOT publicly visible. Braydon can review,
edit, and publish from the admin.

Usage:
    uv run python -m scripts.seo.create_comparison_page          # creates draft
    uv run python -m scripts.seo.create_comparison_page --check  # checks if exists
    uv run python -m scripts.seo.create_comparison_page --update # updates existing draft
"""

import argparse
import json
import logging
import sys

from pipelines import config
from pipelines.shopify.client import ShopifyClient
from scripts.seo._writer_client import get_writer_client
from scripts.seo.comparison_page_content import (
    FAQ_SCHEMA,
    META_DESCRIPTION,
    PAGE_BODY_HTML,
    PAGE_HANDLE,
    PAGE_TITLE,
    SEO_TITLE,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def build_body_with_schema() -> str:
    """Wrap the page body with the FAQ JSON-LD schema script tag."""
    schema_block = (
        f'<script type="application/ld+json">\n'
        f"{json.dumps(FAQ_SCHEMA, indent=2)}\n"
        f"</script>\n"
    )
    return schema_block + PAGE_BODY_HTML


def get_existing_page(client: ShopifyClient) -> dict | None:
    """Find an existing page by handle, if any."""
    resp = client._session.get(
        f"{client.base_url}/pages.json",
        params={"handle": PAGE_HANDLE},
        timeout=30,
    )
    resp.raise_for_status()
    pages = resp.json().get("pages", [])
    return pages[0] if pages else None


def create_draft(client: ShopifyClient) -> dict:
    """Create the page as an unpublished draft."""
    payload = {
        "page": {
            "title": PAGE_TITLE,
            "handle": PAGE_HANDLE,
            "body_html": build_body_with_schema(),
            "published": False,
            "metafields": [
                {
                    "namespace": "global",
                    "key": "title_tag",
                    "value": SEO_TITLE,
                    "type": "single_line_text_field",
                },
                {
                    "namespace": "global",
                    "key": "description_tag",
                    "value": META_DESCRIPTION,
                    "type": "single_line_text_field",
                },
            ],
        }
    }
    resp = client._session.post(
        f"{client.base_url}/pages.json",
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["page"]


def update_existing(client: ShopifyClient, page_id: int) -> dict:
    """Update an existing draft with refreshed content."""
    payload = {
        "page": {
            "id": page_id,
            "title": PAGE_TITLE,
            "body_html": build_body_with_schema(),
            "metafields": [
                {
                    "namespace": "global",
                    "key": "title_tag",
                    "value": SEO_TITLE,
                    "type": "single_line_text_field",
                },
                {
                    "namespace": "global",
                    "key": "description_tag",
                    "value": META_DESCRIPTION,
                    "type": "single_line_text_field",
                },
            ],
        }
    }
    resp = client._session.put(
        f"{client.base_url}/pages/{page_id}.json",
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["page"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Just check if the page exists; don't create or update.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update existing draft (refresh content).",
    )
    args = parser.parse_args()

    client = get_writer_client()

    existing = get_existing_page(client)

    if args.check:
        if existing:
            logger.info(
                f"EXISTS: page id={existing['id']} handle='{existing['handle']}' "
                f"published={existing['published_at'] is not None}"
            )
        else:
            logger.info(f"NOT FOUND: no page with handle='{PAGE_HANDLE}'")
        return 0

    if existing and not args.update:
        logger.warning(
            f"Page already exists (id={existing['id']}). "
            f"Use --update to refresh content, or --check to view status."
        )
        admin_url = (
            f"https://admin.shopify.com/store/"
            f"{config.SHOPIFY_SHOP_DOMAIN.replace('.myshopify.com', '')}"
            f"/pages/{existing['id']}"
        )
        logger.info(f"Admin URL: {admin_url}")
        return 0

    if existing and args.update:
        logger.info(f"Updating existing page id={existing['id']}...")
        page = update_existing(client, existing["id"])
        logger.info("Updated.")
    else:
        logger.info("Creating draft page...")
        page = create_draft(client)
        logger.info(f"Created page id={page['id']}, handle='{page['handle']}'")

    admin_url = (
        f"https://admin.shopify.com/store/"
        f"{config.SHOPIFY_SHOP_DOMAIN.replace('.myshopify.com', '')}"
        f"/pages/{page['id']}"
    )
    public_preview = (
        f"https://pickleballeffectshop.com/pages/{page['handle']}"
    )

    print()
    print("=" * 60)
    print("Page is live as a DRAFT (not publicly visible).")
    print()
    print(f"Title:        {page['title']}")
    print(f"Handle:       {page['handle']}")
    print(f"Published:    {page['published_at'] is not None}")
    print(f"Admin URL:    {admin_url}")
    print(f"Public URL:   {public_preview}  (will 404 until published)")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""dlt pipeline entry point for Shopify."""

import logging

import dlt

from ..config import (
    BIGQUERY_LOCATION,
    GCP_PROJECT_ID,
    PIPELINE_ROLLING_DAYS,
    SHOPIFY_ACCESS_TOKEN,
    SHOPIFY_API_VERSION,
    SHOPIFY_CLIENT_ID,
    SHOPIFY_CLIENT_SECRET,
    SHOPIFY_SHOP_DOMAIN,
)
from .source import shopify_source

logger = logging.getLogger(__name__)


def run_pipeline(
    destination: str = "bigquery",
    dataset_name: str = "raw_shopify",
    days_back: int | None = None,
) -> dlt.pipeline:
    """Run the Shopify dlt pipeline.

    Args:
        destination: "bigquery" or "duckdb"
        dataset_name: Target dataset name
        days_back: Override for rolling window
    """
    if days_back is None:
        days_back = PIPELINE_ROLLING_DAYS

    if destination == "bigquery":
        dest = dlt.destinations.bigquery(
            credentials={
                "project_id": GCP_PROJECT_ID,
                "location": BIGQUERY_LOCATION,
            }
        )
    else:
        dest = destination

    pipeline = dlt.pipeline(
        pipeline_name="shopify",
        destination=dest,
        dataset_name=dataset_name,
    )

    source = shopify_source(
        shop_domain=SHOPIFY_SHOP_DOMAIN,
        client_id=SHOPIFY_CLIENT_ID,
        client_secret=SHOPIFY_CLIENT_SECRET,
        access_token=SHOPIFY_ACCESS_TOKEN,
        api_version=SHOPIFY_API_VERSION,
        days_back=days_back,
    )

    logger.info(
        f"Running Shopify pipeline: dest={destination}, dataset={dataset_name}, "
        f"days={days_back}"
    )

    load_info = pipeline.run(source)
    logger.info(f"Pipeline complete: {load_info}")
    return load_info

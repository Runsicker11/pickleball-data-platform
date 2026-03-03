"""
dlt pipeline entry point for Amazon Seller Central.
"""

import logging

import dlt

from ..config import (
    BIGQUERY_LOCATION,
    GCP_PROJECT_ID,
    SP_API_CLIENT_ID,
    SP_API_CLIENT_SECRET,
    SP_API_MARKETPLACE_ID,
    SP_API_REFRESH_TOKEN,
)
from .source import amazon_seller_source

logger = logging.getLogger(__name__)


def run_pipeline(
    destination: str = "bigquery",
    dataset_name: str = "raw_amazon",
    days_back: int = 30,
) -> dlt.pipeline:
    """Run the Amazon Seller Central dlt pipeline."""

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
        pipeline_name="amazon_seller",
        destination=dest,
        dataset_name=dataset_name,
    )

    source = amazon_seller_source(
        client_id=SP_API_CLIENT_ID,
        client_secret=SP_API_CLIENT_SECRET,
        refresh_token=SP_API_REFRESH_TOKEN,
        marketplace_id=SP_API_MARKETPLACE_ID,
        days_back=days_back,
    )

    logger.info(
        f"Running Amazon Seller pipeline: dest={destination}, dataset={dataset_name}, "
        f"days={days_back}"
    )

    load_info = pipeline.run(source)
    logger.info(f"Pipeline complete: {load_info}")
    return load_info

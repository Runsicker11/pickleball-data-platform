"""dlt pipeline entry point for PayPal."""

import logging

import dlt

from ..config import (
    BIGQUERY_LOCATION,
    GCP_PROJECT_ID,
    PIPELINE_ROLLING_DAYS,
    PAYPAL_CLIENT_ID,
    PAYPAL_CLIENT_SECRET,
)
from .source import paypal_source

logger = logging.getLogger(__name__)


def run_pipeline(
    destination: str = "bigquery",
    dataset_name: str = "raw_paypal",
    days_back: int | None = None,
) -> dlt.pipeline:
    """Run the PayPal dlt pipeline.

    Args:
        destination: "bigquery" or "duckdb"
        dataset_name: Target dataset name
        days_back: Override for rolling window (default: 365 days for full history)
    """
    if days_back is None:
        days_back = max(PIPELINE_ROLLING_DAYS, 365)

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
        pipeline_name="paypal",
        destination=dest,
        dataset_name=dataset_name,
    )

    source = paypal_source(
        client_id=PAYPAL_CLIENT_ID,
        client_secret=PAYPAL_CLIENT_SECRET,
        days_back=days_back,
    )

    logger.info(
        f"Running PayPal pipeline: dest={destination}, dataset={dataset_name}, "
        f"days={days_back}"
    )

    load_info = pipeline.run(source)
    logger.info(f"Pipeline complete: {load_info}")
    return load_info

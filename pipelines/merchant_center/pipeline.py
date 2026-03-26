"""dlt pipeline entry point for Merchant Center."""

import logging

import dlt

from ..config import (
    BIGQUERY_LOCATION,
    GCP_PROJECT_ID,
    MERCHANT_CENTER_ID,
    MERCHANT_CENTER_SA_KEY_JSON,
)
from .source import merchant_center_source

logger = logging.getLogger(__name__)


def run_pipeline(
    destination: str = "bigquery",
    dataset_name: str = "raw_merchant_center",
) -> dlt.pipeline:
    """Run the Merchant Center dlt pipeline."""
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
        pipeline_name="merchant_center",
        destination=dest,
        dataset_name=dataset_name,
    )

    source = merchant_center_source(
        merchant_id=MERCHANT_CENTER_ID,
        sa_key_json=MERCHANT_CENTER_SA_KEY_JSON,
    )

    logger.info(
        f"Running Merchant Center pipeline: dest={destination}, dataset={dataset_name}, "
        f"merchant_id={MERCHANT_CENTER_ID}"
    )

    load_info = pipeline.run(source)
    logger.info(f"Pipeline complete: {load_info}")
    return load_info

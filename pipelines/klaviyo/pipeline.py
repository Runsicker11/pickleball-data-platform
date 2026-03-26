"""dlt pipeline entry point for Klaviyo."""

import logging

import dlt

from ..config import (
    BIGQUERY_LOCATION,
    GCP_PROJECT_ID,
    KLAVIYO_API_KEY,
    PIPELINE_ROLLING_DAYS,
)
from .source import klaviyo_source

logger = logging.getLogger(__name__)


def run_pipeline(
    destination: str = "bigquery",
    dataset_name: str = "raw_klaviyo",
    days_back: int | None = None,
) -> dlt.pipeline:
    """Run the Klaviyo dlt pipeline.

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
        pipeline_name="klaviyo",
        destination=dest,
        dataset_name=dataset_name,
    )

    source = klaviyo_source(
        api_key=KLAVIYO_API_KEY,
        days_back=days_back,
    )

    logger.info(
        f"Running Klaviyo pipeline: dest={destination}, dataset={dataset_name}, "
        f"days={days_back}"
    )

    load_info = pipeline.run(source)
    logger.info(f"Pipeline complete: {load_info}")
    return load_info

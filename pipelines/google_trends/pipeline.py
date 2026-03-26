"""dlt pipeline entry point for Google Trends."""

import logging

import dlt

from ..config import BIGQUERY_LOCATION, GCP_PROJECT_ID
from .source import google_trends_source

logger = logging.getLogger(__name__)


def run_pipeline(
    destination: str = "bigquery",
    dataset_name: str = "raw_google_trends",
) -> dlt.pipeline:
    """Run the Google Trends dlt pipeline (always pulls full 5-year weekly history)."""
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
        pipeline_name="google_trends",
        destination=dest,
        dataset_name=dataset_name,
    )

    source = google_trends_source()

    logger.info(f"Running Google Trends pipeline: dest={destination}, dataset={dataset_name}")

    load_info = pipeline.run(source)
    logger.info(f"Pipeline complete: {load_info}")
    return load_info

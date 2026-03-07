"""dlt pipeline entry point for Meta Ads."""

import logging

import dlt

from ..config import (
    BIGQUERY_LOCATION,
    GCP_PROJECT_ID,
    META_ACCESS_TOKEN,
    META_ADS_ACCOUNT_ID,
    META_API_VERSION,
    META_APP_ID,
    META_APP_SECRET,
    PIPELINE_ROLLING_DAYS,
)
from .source import meta_ads_source

logger = logging.getLogger(__name__)


def run_pipeline(
    destination: str = "bigquery",
    dataset_name: str = "raw_meta",
    days_back: int | None = None,
) -> dlt.pipeline:
    """Run the Meta Ads dlt pipeline."""
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
        pipeline_name="meta_ads",
        destination=dest,
        dataset_name=dataset_name,
    )

    source = meta_ads_source(
        account_id=META_ADS_ACCOUNT_ID,
        access_token=META_ACCESS_TOKEN,
        app_id=META_APP_ID,
        app_secret=META_APP_SECRET,
        api_version=META_API_VERSION,
        days_back=days_back,
    )

    logger.info(
        f"Running Meta Ads pipeline: dest={destination}, dataset={dataset_name}, "
        f"days={days_back}"
    )

    load_info = pipeline.run(source)
    logger.info(f"Pipeline complete: {load_info}")
    return load_info

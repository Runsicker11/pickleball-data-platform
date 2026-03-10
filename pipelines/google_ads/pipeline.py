"""dlt pipeline entry point for Google Ads."""

import logging

import dlt

from ..config import (
    BIGQUERY_LOCATION,
    GCP_PROJECT_ID,
    GOOGLE_ADS_CLIENT_ID,
    GOOGLE_ADS_CLIENT_SECRET,
    GOOGLE_ADS_CUSTOMER_ID,
    GOOGLE_ADS_DEVELOPER_TOKEN,
    GOOGLE_ADS_LOGIN_CUSTOMER_ID,
    GOOGLE_ADS_REFRESH_TOKEN,
    PIPELINE_ROLLING_DAYS,
)
from .source import google_ads_source

logger = logging.getLogger(__name__)


def run_pipeline(
    destination: str = "bigquery",
    dataset_name: str = "raw_google_ads",
    days_back: int | None = None,
) -> dlt.pipeline:
    """Run the Google Ads dlt pipeline."""
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
        pipeline_name="google_ads",
        destination=dest,
        dataset_name=dataset_name,
    )

    source = google_ads_source(
        customer_id=GOOGLE_ADS_CUSTOMER_ID,
        developer_token=GOOGLE_ADS_DEVELOPER_TOKEN,
        client_id=GOOGLE_ADS_CLIENT_ID,
        client_secret=GOOGLE_ADS_CLIENT_SECRET,
        refresh_token=GOOGLE_ADS_REFRESH_TOKEN,
        login_customer_id=GOOGLE_ADS_LOGIN_CUSTOMER_ID,
        days_back=days_back,
    )

    logger.info(
        f"Running Google Ads pipeline: dest={destination}, dataset={dataset_name}, "
        f"days={days_back}"
    )

    load_info = pipeline.run(source)
    logger.info(f"Pipeline complete: {load_info}")
    return load_info

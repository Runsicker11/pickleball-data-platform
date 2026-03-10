"""dlt pipeline entry point for Search Console."""

import logging

import dlt

from ..config import (
    BIGQUERY_LOCATION,
    GCP_PROJECT_ID,
    GOOGLE_SEARCH_CONSOLE_CLIENT_ID,
    GOOGLE_SEARCH_CONSOLE_CLIENT_SECRET,
    GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN,
    GOOGLE_SEARCH_CONSOLE_SITE_URL,
    GOOGLE_SEARCH_CONSOLE_SITE_URL_SHOP,
    PIPELINE_ROLLING_DAYS,
)
from .source import search_console_source

logger = logging.getLogger(__name__)


def run_pipeline(
    destination: str = "bigquery",
    dataset_name: str = "raw_search_console",
    days_back: int | None = None,
) -> dlt.pipeline:
    """Run the Search Console dlt pipeline."""
    if days_back is None:
        days_back = max(PIPELINE_ROLLING_DAYS, 7)  # SC needs at least 7 days

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
        pipeline_name="search_console",
        destination=dest,
        dataset_name=dataset_name,
    )

    source = search_console_source(
        client_id=GOOGLE_SEARCH_CONSOLE_CLIENT_ID,
        client_secret=GOOGLE_SEARCH_CONSOLE_CLIENT_SECRET,
        refresh_token=GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN,
        site_url=GOOGLE_SEARCH_CONSOLE_SITE_URL,
        site_url_shop=GOOGLE_SEARCH_CONSOLE_SITE_URL_SHOP,
        days_back=days_back,
    )

    logger.info(
        f"Running Search Console pipeline: dest={destination}, dataset={dataset_name}, "
        f"days={days_back}"
    )

    load_info = pipeline.run(source)
    logger.info(f"Pipeline complete: {load_info}")
    return load_info

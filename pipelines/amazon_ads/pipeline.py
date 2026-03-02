"""
dlt pipeline entry point for Amazon Ads.
"""

import logging

import dlt

from ..config import (
    AMAZON_CLIENT_ID,
    AMAZON_CLIENT_SECRET,
    AMAZON_PROFILE_IDS,
    AMAZON_REFRESH_TOKEN,
    BIGQUERY_LOCATION,
    GCP_PROJECT_ID,
    PIPELINE_POLL_INTERVAL,
    PIPELINE_POLL_TIMEOUT_MINUTES,
    PIPELINE_ROLLING_DAYS,
)
from .report_configs import ALL_REPORTS, ReportConfig
from .source import amazon_ads_source

logger = logging.getLogger(__name__)


def run_pipeline(
    destination: str = "bigquery",
    dataset_name: str = "raw_amazon",
    days_back: int | None = None,
    reports: list[ReportConfig] | None = None,
) -> dlt.pipeline:
    """Run the Amazon Ads dlt pipeline.

    Args:
        destination: "bigquery" or "duckdb"
        dataset_name: Target dataset name
        days_back: Override for rolling window
        reports: Subset of reports to run; defaults to ALL_REPORTS
    """
    if days_back is None:
        days_back = PIPELINE_ROLLING_DAYS
    if reports is None:
        reports = ALL_REPORTS

    # Build destination
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
        pipeline_name="amazon_ads",
        destination=dest,
        dataset_name=dataset_name,
    )

    source = amazon_ads_source(
        client_id=AMAZON_CLIENT_ID,
        client_secret=AMAZON_CLIENT_SECRET,
        refresh_token=AMAZON_REFRESH_TOKEN,
        profile_ids=AMAZON_PROFILE_IDS,
        days_back=days_back,
        poll_interval=PIPELINE_POLL_INTERVAL,
        poll_timeout_minutes=PIPELINE_POLL_TIMEOUT_MINUTES,
        reports=reports,
    )

    logger.info(
        f"Running Amazon Ads pipeline: dest={destination}, dataset={dataset_name}, "
        f"days={days_back}, reports={[r.name for r in reports]}"
    )

    load_info = pipeline.run(source)
    logger.info(f"Pipeline complete: {load_info}")
    return load_info

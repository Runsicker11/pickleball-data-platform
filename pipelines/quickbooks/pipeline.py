"""dlt pipeline entry point for QuickBooks Online."""

import logging

import dlt

from ..config import (
    BIGQUERY_LOCATION,
    GCP_PROJECT_ID,
    PIPELINE_ROLLING_DAYS,
    QUICKBOOKS_CLIENT_ID,
    QUICKBOOKS_CLIENT_SECRET,
    QUICKBOOKS_REALM_ID,
    QUICKBOOKS_REFRESH_TOKEN,
)
from .source import quickbooks_source

logger = logging.getLogger(__name__)


def run_pipeline(
    destination: str = "bigquery",
    dataset_name: str = "raw_quickbooks",
    days_back: int | None = None,
) -> dlt.pipeline:
    """Run the QuickBooks dlt pipeline.

    Args:
        destination: "bigquery" or "duckdb"
        dataset_name: Target dataset name
        days_back: Override for rolling window (default: 90 days for accounting data)
    """
    if days_back is None:
        days_back = max(PIPELINE_ROLLING_DAYS, 90)  # accounting data needs wider window

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
        pipeline_name="quickbooks",
        destination=dest,
        dataset_name=dataset_name,
    )

    source = quickbooks_source(
        client_id=QUICKBOOKS_CLIENT_ID,
        client_secret=QUICKBOOKS_CLIENT_SECRET,
        refresh_token=QUICKBOOKS_REFRESH_TOKEN,
        realm_id=QUICKBOOKS_REALM_ID,
        days_back=days_back,
    )

    logger.info(
        f"Running QuickBooks pipeline: dest={destination}, dataset={dataset_name}, "
        f"days={days_back}"
    )

    load_info = pipeline.run(source)
    logger.info(f"Pipeline complete: {load_info}")
    return load_info

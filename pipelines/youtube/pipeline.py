"""dlt pipeline entry point for YouTube."""

import logging

import dlt

from ..config import (
    BIGQUERY_LOCATION,
    GCP_PROJECT_ID,
    PIPELINE_ROLLING_DAYS,
    YOUTUBE_CHANNEL_ID,
    YOUTUBE_REFRESH_TOKEN,
)
from .source import youtube_source

logger = logging.getLogger(__name__)


def run_pipeline(
    destination: str = "bigquery",
    dataset_name: str = "raw_youtube",
    days_back: int | None = None,
) -> dlt.pipeline:
    """Run the YouTube dlt pipeline."""
    if not YOUTUBE_REFRESH_TOKEN:
        raise RuntimeError(
            "YOUTUBE_REFRESH_TOKEN is not set. "
            "Run `python -m pipelines.youtube.auth` to generate one."
        )
    if not YOUTUBE_CHANNEL_ID:
        raise RuntimeError("YOUTUBE_CHANNEL_ID is not set in .env")

    if days_back is None:
        days_back = max(PIPELINE_ROLLING_DAYS, 7)

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
        pipeline_name="youtube",
        destination=dest,
        dataset_name=dataset_name,
    )

    source = youtube_source(
        refresh_token=YOUTUBE_REFRESH_TOKEN,
        channel_id=YOUTUBE_CHANNEL_ID,
        days_back=days_back,
    )

    logger.info(
        f"Running YouTube pipeline: dest={destination}, dataset={dataset_name}, "
        f"days={days_back}, channel={YOUTUBE_CHANNEL_ID}"
    )

    load_info = pipeline.run(source)
    logger.info(f"Pipeline complete: {load_info}")
    return load_info

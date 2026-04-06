"""
Historical backfill for YouTube data.

Pulls full history since channel launch:
  - channel_daily_analytics: one call for the full date range
  - video_analytics: chunked in 90-day windows so each chunk gets
    its own top-200-by-views ranking (better coverage than one big window)

Usage:
    python -m pipelines.youtube.backfill
    python -m pipelines.youtube.backfill --start 2022-01-01   # partial backfill
    python -m pipelines.youtube.backfill --destination duckdb  # test locally
"""

import argparse
import logging
from datetime import date, datetime, timedelta, timezone

import dlt

from ..config import (
    BIGQUERY_LOCATION,
    GCP_PROJECT_ID,
    YOUTUBE_CHANNEL_ID,
    YOUTUBE_REFRESH_TOKEN,
)
from .client import YouTubeClient

logger = logging.getLogger(__name__)

CHANNEL_LAUNCH_DATE = date(2021, 12, 1)
CHUNK_DAYS = 90


def _make_pipeline(destination: str, dataset_name: str):
    if destination == "bigquery":
        dest = dlt.destinations.bigquery(
            credentials={"project_id": GCP_PROJECT_ID, "location": BIGQUERY_LOCATION}
        )
    else:
        dest = destination
    return dlt.pipeline(
        pipeline_name="youtube_backfill",
        destination=dest,
        dataset_name=dataset_name,
    )


def _date_chunks(start: date, end: date, chunk_days: int):
    """Yield (chunk_start, chunk_end) tuples covering start→end."""
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def run_backfill(
    start_date: date = CHANNEL_LAUNCH_DATE,
    destination: str = "bigquery",
    dataset_name: str = "raw_youtube",
) -> None:
    if not YOUTUBE_REFRESH_TOKEN:
        raise RuntimeError(
            "YOUTUBE_REFRESH_TOKEN not set. Run `python -m pipelines.youtube.auth` first."
        )

    client = YouTubeClient(refresh_token=YOUTUBE_REFRESH_TOKEN, channel_id=YOUTUBE_CHANNEL_ID)
    client.validate_access()

    yesterday = date.today() - timedelta(days=1)
    pipeline = _make_pipeline(destination, dataset_name)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # ── 1. Channel daily analytics — full range in one call ──────────────
    logger.info(f"Backfilling channel_daily_analytics: {start_date} → {yesterday}")

    @dlt.resource(
        name="channel_daily_analytics",
        write_disposition="merge",
        primary_key=["channel_id", "report_date"],
        merge_key=["channel_id", "report_date"],
    )
    def channel_daily_full():
        rows = client.get_channel_daily_analytics(start_date, yesterday)
        for row in rows:
            yield {**row, "ingested_at": now_str}
        logger.info(f"  → {len(rows)} days loaded")

    @dlt.source(name="youtube")
    def channel_source():
        yield channel_daily_full

    pipeline.run(channel_source())
    logger.info("Channel daily analytics: done.")

    # ── 2. Video analytics — fetch all 90-day chunks, then load at once ─
    chunks = list(_date_chunks(start_date, yesterday, CHUNK_DAYS))
    logger.info(
        f"Backfilling video_analytics in {len(chunks)} chunks of {CHUNK_DAYS} days "
        f"({start_date} → {yesterday})"
    )

    all_video_rows = []
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        logger.info(f"  Fetching chunk {i}/{len(chunks)}: {chunk_start} → {chunk_end}")
        rows = client.get_video_analytics(chunk_start, chunk_end)
        all_video_rows.extend(rows)
        logger.info(f"    → {len(rows)} rows (running total: {len(all_video_rows)})")

    logger.info(f"Loading {len(all_video_rows)} total video_analytics rows to {destination}...")

    @dlt.resource(
        name="video_analytics",
        write_disposition="merge",
        primary_key=["video_id", "period_start", "period_end"],
        merge_key=["video_id", "period_start", "period_end"],
    )
    def all_video_analytics():
        for row in all_video_rows:
            yield {**row, "ingested_at": now_str}

    @dlt.source(name="youtube")
    def video_source():
        yield all_video_analytics

    pipeline.run(video_source())

    logger.info("Backfill complete.")
    logger.info(
        f"Summary: {(yesterday - start_date).days + 1} days of channel history, "
        f"{len(all_video_rows)} video-period rows across {len(chunks)} x {CHUNK_DAYS}-day chunks."
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="YouTube historical backfill")
    parser.add_argument(
        "--start",
        default=CHANNEL_LAUNCH_DATE.isoformat(),
        help=f"Start date (default: {CHANNEL_LAUNCH_DATE})",
    )
    parser.add_argument(
        "--destination",
        choices=["bigquery", "duckdb"],
        default="bigquery",
    )
    parser.add_argument(
        "--dataset",
        default="raw_youtube",
    )
    args = parser.parse_args()

    run_backfill(
        start_date=date.fromisoformat(args.start),
        destination=args.destination,
        dataset_name=args.dataset,
    )

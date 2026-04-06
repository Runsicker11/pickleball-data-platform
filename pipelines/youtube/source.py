"""dlt source for YouTube — 4 resources: channel_stats, videos, video_stats, video_analytics."""

import logging
from datetime import date, datetime, timedelta, timezone

import dlt

from .client import YouTubeClient

logger = logging.getLogger(__name__)


@dlt.source(name="youtube")
def youtube_source(
    refresh_token: str = dlt.secrets.value,
    channel_id: str = dlt.secrets.value,
    days_back: int = 30,
):
    """dlt source yielding 4 YouTube resources."""
    client = YouTubeClient(refresh_token=refresh_token, channel_id=channel_id)
    client.validate_access()

    end_date = date.today() - timedelta(days=1)  # Analytics lags 1 day
    start_date = end_date - timedelta(days=days_back - 1)

    yield _channel_stats_resource(client)
    yield _videos_resource(client)
    yield _video_stats_resource(client)
    yield _video_analytics_resource(client, start_date, end_date)
    yield _channel_daily_analytics_resource(client, start_date, end_date)


def _channel_stats_resource(client: YouTubeClient):
    @dlt.resource(
        name="channel_stats",
        write_disposition="merge",
        primary_key=["channel_id", "snapshot_date"],
        merge_key=["channel_id", "snapshot_date"],
    )
    def channel_stats():
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        today = date.today().isoformat()

        stats = client.get_channel_stats()
        if stats:
            yield {
                **stats,
                "snapshot_date": today,
                "ingested_at": now_str,
            }

    return channel_stats


def _videos_resource(client: YouTubeClient):
    @dlt.resource(
        name="videos",
        write_disposition="merge",
        primary_key="video_id",
        merge_key="video_id",
    )
    def videos():
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        video_ids = client.list_all_video_ids()
        metadata = client.get_video_metadata(video_ids)

        for row in metadata:
            yield {**row, "ingested_at": now_str}

        logger.info(f"Videos: yielded {len(metadata)} video records")

    return videos


def _video_stats_resource(client: YouTubeClient):
    @dlt.resource(
        name="video_stats",
        write_disposition="merge",
        primary_key=["video_id", "snapshot_date"],
        merge_key=["video_id", "snapshot_date"],
    )
    def video_stats():
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        today = date.today().isoformat()

        video_ids = client.list_all_video_ids()
        stats = client.get_video_stats(video_ids)

        for row in stats:
            yield {
                **row,
                "snapshot_date": today,
                "ingested_at": now_str,
            }

        logger.info(f"Video stats: yielded {len(stats)} rows")

    return video_stats


def _video_analytics_resource(client: YouTubeClient, start_date: date, end_date: date):
    @dlt.resource(
        name="video_analytics",
        write_disposition="merge",
        primary_key=["video_id", "period_start", "period_end"],
        merge_key=["video_id", "period_start", "period_end"],
    )
    def video_analytics():
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        rows = client.get_video_analytics(start_date, end_date)
        for row in rows:
            yield {**row, "ingested_at": now_str}

        logger.info(f"Video analytics: yielded {len(rows)} rows ({start_date} to {end_date})")

    return video_analytics


def _channel_daily_analytics_resource(client: YouTubeClient, start_date: date, end_date: date):
    @dlt.resource(
        name="channel_daily_analytics",
        write_disposition="merge",
        primary_key=["channel_id", "report_date"],
        merge_key=["channel_id", "report_date"],
    )
    def channel_daily_analytics():
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        rows = client.get_channel_daily_analytics(start_date, end_date)
        for row in rows:
            yield {**row, "ingested_at": now_str}

        logger.info(f"Channel daily analytics: yielded {len(rows)} days ({start_date} to {end_date})")

    return channel_daily_analytics

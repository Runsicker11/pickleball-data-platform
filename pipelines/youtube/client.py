"""YouTube Data API v3 + Analytics API client."""

import logging
from datetime import date, timedelta

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from .auth import load_client_credentials

logger = logging.getLogger(__name__)

_DATA_SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube",
]

_VIDEO_BATCH_SIZE = 50  # YouTube API max per videos.list call


class YouTubeClient:
    """Client for YouTube Data API v3 and YouTube Analytics API."""

    def __init__(self, refresh_token: str, channel_id: str):
        client_id, client_secret = load_client_credentials()

        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=_DATA_SCOPES,
        )

        self._yt = build("youtube", "v3", credentials=creds)
        self._analytics = build("youtubeAnalytics", "v2", credentials=creds)
        self.channel_id = channel_id

    def validate_access(self) -> None:
        """Verify the channel is accessible and credentials are valid."""
        resp = (
            self._yt.channels()
            .list(part="id,snippet", id=self.channel_id)
            .execute()
        )
        items = resp.get("items", [])
        if not items:
            raise RuntimeError(
                f"Channel {self.channel_id!r} not found or not accessible. "
                "Check YOUTUBE_CHANNEL_ID and that OAuth scopes are correct."
            )
        title = items[0]["snippet"]["title"]
        logger.info(f"YouTube access verified: channel={title!r} ({self.channel_id})")

    def get_channel_stats(self) -> dict:
        """Return current channel-level statistics."""
        resp = (
            self._yt.channels()
            .list(part="statistics,snippet", id=self.channel_id)
            .execute()
        )
        items = resp.get("items", [])
        if not items:
            return {}
        item = items[0]
        stats = item.get("statistics", {})
        snippet = item.get("snippet", {})
        return {
            "channel_id": self.channel_id,
            "title": snippet.get("title"),
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "view_count": int(stats.get("viewCount", 0)),
            "video_count": int(stats.get("videoCount", 0)),
            "hidden_subscriber_count": stats.get("hiddenSubscriberCount", False),
        }

    def list_all_video_ids(self) -> list[str]:
        """Return all video IDs uploaded to the channel (newest first)."""
        # Get the uploads playlist ID
        resp = (
            self._yt.channels()
            .list(part="contentDetails", id=self.channel_id)
            .execute()
        )
        uploads_playlist_id = (
            resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        )

        video_ids = []
        page_token = None

        while True:
            params = {
                "part": "contentDetails",
                "playlistId": uploads_playlist_id,
                "maxResults": 50,
            }
            if page_token:
                params["pageToken"] = page_token

            resp = self._yt.playlistItems().list(**params).execute()

            for item in resp.get("items", []):
                video_ids.append(item["contentDetails"]["videoId"])

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        logger.info(f"Found {len(video_ids)} total videos for channel {self.channel_id}")
        return video_ids

    def get_video_metadata(self, video_ids: list[str]) -> list[dict]:
        """Fetch snippet + contentDetails for a list of video IDs."""
        results = []

        for i in range(0, len(video_ids), _VIDEO_BATCH_SIZE):
            batch = video_ids[i : i + _VIDEO_BATCH_SIZE]
            resp = (
                self._yt.videos()
                .list(
                    part="snippet,contentDetails,status",
                    id=",".join(batch),
                )
                .execute()
            )
            for item in resp.get("items", []):
                snippet = item.get("snippet", {})
                content = item.get("contentDetails", {})
                status = item.get("status", {})
                results.append({
                    "video_id": item["id"],
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "published_at": snippet.get("publishedAt"),
                    "channel_id": snippet.get("channelId"),
                    "tags": ",".join(snippet.get("tags", [])),
                    "category_id": snippet.get("categoryId"),
                    "duration": content.get("duration"),  # ISO 8601 e.g. PT12M3S
                    "thumbnail_url": (
                        snippet.get("thumbnails", {})
                        .get("high", {})
                        .get("url")
                    ),
                    "privacy_status": status.get("privacyStatus"),
                    "made_for_kids": status.get("madeForKids", False),
                })

        return results

    def get_video_stats(self, video_ids: list[str]) -> list[dict]:
        """Fetch current engagement stats (views, likes, comments) for a list of video IDs."""
        results = []

        for i in range(0, len(video_ids), _VIDEO_BATCH_SIZE):
            batch = video_ids[i : i + _VIDEO_BATCH_SIZE]
            resp = (
                self._yt.videos()
                .list(part="statistics", id=",".join(batch))
                .execute()
            )
            for item in resp.get("items", []):
                stats = item.get("statistics", {})
                results.append({
                    "video_id": item["id"],
                    "view_count": int(stats.get("viewCount", 0)),
                    "like_count": int(stats.get("likeCount", 0)),
                    "comment_count": int(stats.get("commentCount", 0)),
                    "favorite_count": int(stats.get("favoriteCount", 0)),
                })

        return results

    def get_video_analytics(
        self,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        Fetch per-video analytics totals for a date range from the YouTube Analytics API.
        Returns one row per video_id (aggregated over the date range).
        """
        resp = (
            self._analytics.reports()
            .query(
                ids=f"channel=={self.channel_id}",
                startDate=start_date.isoformat(),
                endDate=end_date.isoformat(),
                dimensions="video",
                metrics=(
                    "views,"
                    "estimatedMinutesWatched,"
                    "averageViewDuration,"
                    "averageViewPercentage,"
                    "likes,"
                    "comments,"
                    "shares,"
                    "subscribersGained,"
                    "subscribersLost"
                ),
                sort="-views",
                maxResults=200,  # API hard cap; returns top 200 videos by views
            )
            .execute()
        )

        headers = [col["name"] for col in resp.get("columnHeaders", [])]
        rows = resp.get("rows") or []

        results = []
        for row in rows:
            record = dict(zip(headers, row))
            results.append({
                "video_id": record.get("video"),
                "period_start": start_date.isoformat(),
                "period_end": end_date.isoformat(),
                "views": int(record.get("views", 0)),
                "estimated_minutes_watched": float(record.get("estimatedMinutesWatched", 0)),
                "avg_view_duration_seconds": float(record.get("averageViewDuration", 0)),
                "avg_view_percentage": float(record.get("averageViewPercentage", 0)),
                "likes": int(record.get("likes", 0)),
                "comments": int(record.get("comments", 0)),
                "shares": int(record.get("shares", 0)),
                "subscribers_gained": int(record.get("subscribersGained", 0)),
                "subscribers_lost": int(record.get("subscribersLost", 0)),
            })

        logger.info(
            f"Video analytics: {len(results)} videos ({start_date} to {end_date})"
        )
        return results

    def get_channel_daily_analytics(
        self,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        Fetch channel-level daily analytics (views, watch time, subscribers).
        Returns one row per day.
        """
        resp = (
            self._analytics.reports()
            .query(
                ids=f"channel=={self.channel_id}",
                startDate=start_date.isoformat(),
                endDate=end_date.isoformat(),
                dimensions="day",
                metrics=(
                    "views,"
                    "estimatedMinutesWatched,"
                    "averageViewDuration,"
                    "subscribersGained,"
                    "subscribersLost"
                ),
                sort="day",
            )
            .execute()
        )

        headers = [col["name"] for col in resp.get("columnHeaders", [])]
        rows = resp.get("rows") or []

        results = []
        for row in rows:
            record = dict(zip(headers, row))
            results.append({
                "report_date": record.get("day"),
                "channel_id": self.channel_id,
                "views": int(record.get("views", 0)),
                "estimated_minutes_watched": float(record.get("estimatedMinutesWatched", 0)),
                "avg_view_duration_seconds": float(record.get("averageViewDuration", 0)),
                "subscribers_gained": int(record.get("subscribersGained", 0)),
                "subscribers_lost": int(record.get("subscribersLost", 0)),
            })

        logger.info(f"Channel daily analytics: {len(results)} days")
        return results

    def update_video_description(self, video_id: str, title: str, new_description: str) -> None:
        """Update a video's description (used by UTM manager)."""
        self._yt.videos().update(
            part="snippet",
            body={
                "id": video_id,
                "snippet": {
                    "title": title,
                    "description": new_description,
                    "categoryId": "17",  # Sports — required field even when unchanged
                },
            },
        ).execute()
        logger.info(f"Updated description for video {video_id}")

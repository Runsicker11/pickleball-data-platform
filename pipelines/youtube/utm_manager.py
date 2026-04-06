"""
UTM parameter manager for YouTube video descriptions.

Finds links to configured site domains in video descriptions and appends
UTM parameters so GA4 can attribute traffic to the correct video.

UTM convention:
    utm_source=youtube
    utm_medium=video
    utm_campaign=<channel_slug>   (e.g. "pickleball_effect")
    utm_content=<video_id>        (identifies the specific video)

Usage:
    # Dry run — print what would change, don't actually update
    python -m pipelines.youtube.utm_manager

    # Apply changes
    python -m pipelines.youtube.utm_manager --apply

    # Apply only to a specific video
    python -m pipelines.youtube.utm_manager --apply --video-id dQw4w9WgXcQ
"""

import argparse
import logging
import re
import sys
from urllib.parse import ParseResult, parse_qs, urlencode, urlparse, urlunparse

from ..config import YOUTUBE_CHANNEL_ID, YOUTUBE_REFRESH_TOKEN
from .client import YouTubeClient

logger = logging.getLogger(__name__)

# Domains whose links should receive UTM parameters.
# Add both the Shopify store and the review site.
TRACKED_DOMAINS = [
    "pickleballeffect.com",
    "pickleballreview.com",  # update if the review site domain differs
]

UTM_SOURCE = "youtube"
UTM_MEDIUM = "video"
UTM_CAMPAIGN = "pickleball_effect"  # channel-level campaign slug

# Regex to find URLs in plain text
_URL_RE = re.compile(r"https?://[^\s\)\"'<>]+")


def _needs_utm(url: str) -> bool:
    """Return True if the URL points to a tracked domain and is missing utm_source."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower().lstrip("www.")
    if not any(domain.endswith(d) for d in TRACKED_DOMAINS):
        return False
    params = parse_qs(parsed.query)
    return "utm_source" not in params


def _add_utm(url: str, video_id: str) -> str:
    """Append UTM parameters to a URL, preserving any existing query params."""
    parsed: ParseResult = urlparse(url)
    existing = parse_qs(parsed.query, keep_blank_values=True)

    utm_params = {
        "utm_source": [UTM_SOURCE],
        "utm_medium": [UTM_MEDIUM],
        "utm_campaign": [UTM_CAMPAIGN],
        "utm_content": [video_id],
    }
    merged = {**existing, **utm_params}

    new_query = urlencode(merged, doseq=True)
    new_parsed = parsed._replace(query=new_query)
    return urlunparse(new_parsed)


def inject_utms(description: str, video_id: str) -> tuple[str, int]:
    """
    Replace all tracked-domain links in a description with UTM-tagged versions.
    Returns (new_description, number_of_links_updated).
    """
    updates = 0

    def replace_url(match: re.Match) -> str:
        nonlocal updates
        url = match.group(0)
        if _needs_utm(url):
            updates += 1
            return _add_utm(url, video_id)
        return url

    new_description = _URL_RE.sub(replace_url, description)
    return new_description, updates


def run_utm_manager(apply: bool = False, video_id_filter: str | None = None) -> None:
    """
    Scan all channel videos and inject UTMs into descriptions where missing.

    Args:
        apply: If False (dry run), print changes without making them.
        video_id_filter: If set, process only this video ID.
    """
    if not YOUTUBE_REFRESH_TOKEN:
        print(
            "ERROR: YOUTUBE_REFRESH_TOKEN not set. "
            "Run `python -m pipelines.youtube.auth` first.",
            file=sys.stderr,
        )
        sys.exit(1)

    client = YouTubeClient(refresh_token=YOUTUBE_REFRESH_TOKEN, channel_id=YOUTUBE_CHANNEL_ID)
    client.validate_access()

    if video_id_filter:
        video_ids = [video_id_filter]
    else:
        video_ids = client.list_all_video_ids()

    videos = client.get_video_metadata(video_ids)

    updated = 0
    skipped = 0
    no_links = 0

    for video in videos:
        vid = video["video_id"]
        title = video["title"]
        description = video.get("description") or ""

        new_description, n_links = inject_utms(description, vid)

        if n_links == 0:
            no_links += 1
            continue

        print(f"\n{'[DRY RUN] ' if not apply else ''}Video: {title!r} ({vid})")
        print(f"  Links updated: {n_links}")

        if apply:
            try:
                client.update_video_description(vid, title, new_description)
                updated += 1
            except Exception as exc:
                logger.error(f"Failed to update {vid}: {exc}")
                skipped += 1
        else:
            # Show what would change
            for match in _URL_RE.finditer(description):
                url = match.group(0)
                if _needs_utm(url):
                    print(f"  BEFORE: {url}")
                    print(f"  AFTER:  {_add_utm(url, vid)}")

    print(f"\n{'Applied' if apply else 'Dry run'} complete.")
    print(f"  Videos with links to update: {updated + (len(videos) - no_links - skipped) if not apply else updated}")
    print(f"  Videos with no tracked links: {no_links}")
    if skipped:
        print(f"  Errors: {skipped}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="YouTube UTM parameter manager")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually update video descriptions (default: dry run)",
    )
    parser.add_argument(
        "--video-id",
        dest="video_id",
        help="Process only this video ID (default: all videos)",
    )
    args = parser.parse_args()

    run_utm_manager(apply=args.apply, video_id_filter=args.video_id)

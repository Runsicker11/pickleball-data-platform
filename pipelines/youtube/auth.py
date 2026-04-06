"""
One-time OAuth2 authorization flow to generate a refresh token.

Run this locally once to get the YOUTUBE_REFRESH_TOKEN for your .env:

    python -m pipelines.youtube.auth

A browser window will open asking you to authorize the app. After approving,
the refresh token is printed — add it to your .env as YOUTUBE_REFRESH_TOKEN.

Scopes requested:
  - youtube.readonly    → read channel/video data for the pipeline
  - yt-analytics.readonly → read Analytics API (views, watch time, CTR)
  - youtube             → write access to update video descriptions (UTM manager)
"""

import json
import os
import sys
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube",  # needed for UTM description updates
]

_OAUTH_CLIENT_JSON = Path(__file__).parent / "oauth_client.json"


def run_auth_flow() -> str:
    """Run the OAuth2 installed-app flow and return the refresh token."""
    if not _OAUTH_CLIENT_JSON.exists():
        print(f"ERROR: {_OAUTH_CLIENT_JSON} not found.", file=sys.stderr)
        print("Place your OAuth2 client JSON file at that path and try again.", file=sys.stderr)
        sys.exit(1)

    flow = InstalledAppFlow.from_client_secrets_file(str(_OAUTH_CLIENT_JSON), scopes=SCOPES)
    creds = flow.run_local_server(port=0, open_browser=True)

    return creds.refresh_token


def load_client_credentials() -> tuple[str, str]:
    """Load client_id and client_secret from oauth_client.json or env vars."""
    if _OAUTH_CLIENT_JSON.exists():
        with open(_OAUTH_CLIENT_JSON) as f:
            data = json.load(f)
        installed = data.get("installed", data.get("web", {}))
        return installed["client_id"], installed["client_secret"]

    # Fallback: read from environment (used in Cloud Run where the JSON isn't present)
    client_id = os.getenv("YOUTUBE_CLIENT_ID", "")
    client_secret = os.getenv("YOUTUBE_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError(
            "YouTube OAuth credentials not found. Either place oauth_client.json at "
            f"{_OAUTH_CLIENT_JSON} or set YOUTUBE_CLIENT_ID and YOUTUBE_CLIENT_SECRET env vars."
        )
    return client_id, client_secret


if __name__ == "__main__":
    print("Starting YouTube OAuth2 authorization flow...")
    print(f"Using credentials from: {_OAUTH_CLIENT_JSON}\n")

    refresh_token = run_auth_flow()

    print("\n" + "=" * 60)
    print("SUCCESS — add this to your .env file:")
    print(f"\nYOUTUBE_REFRESH_TOKEN={refresh_token}\n")
    print("=" * 60)

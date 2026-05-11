"""Meta Graph API client with token validation and cursor pagination."""

import logging
import urllib.parse
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

_FIELDS_FULL_CREATIVES = (
    "id,name,title,body,call_to_action_type,"
    "object_story_spec,thumbnail_url,image_url,object_type"
)
_FIELDS_BASIC_CREATIVES = (
    "id,name,title,body,call_to_action_type,thumbnail_url,image_url,object_type"
)


def _strip_object_story_spec(url: str) -> str:
    """Remove ``object_story_spec`` from the ``fields`` query param of a Meta paginated URL."""
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    fields = qs.get("fields", [""])[0]
    if "object_story_spec" in fields:
        qs["fields"] = [
            ",".join(f for f in fields.split(",") if f != "object_story_spec")
        ]
    return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(qs, doseq=True)))


def _set_limit(url: str, limit: int) -> str:
    """Replace the ``limit`` query param in a Meta paginated URL."""
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    qs["limit"] = [str(limit)]
    return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(qs, doseq=True)))


def _extract_cursor(url: str) -> str:
    """Best-effort extraction of the ``after`` cursor for log messages."""
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    return qs.get("after", ["<none>"])[0]


class MetaAdsClient:
    """Meta Ads Graph API client."""

    def __init__(
        self,
        account_id: str,
        access_token: str,
        app_id: str = "",
        app_secret: str = "",
        api_version: str = "v21.0",
    ):
        self.account_id = account_id
        self.access_token = access_token
        self.app_id = app_id
        self.app_secret = app_secret
        self.api_version = api_version
        self._session = requests.Session()

    def _api_url(self, path: str) -> str:
        return f"https://graph.facebook.com/{self.api_version}/{path}"

    def validate_token(self) -> dict:
        """Check token validity and warn if near expiration."""
        url = self._api_url("debug_token")
        app_token = f"{self.app_id}|{self.app_secret}"
        resp = self._session.get(
            url,
            params={"input_token": self.access_token, "access_token": app_token},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})

        if not data.get("is_valid", False):
            error_msg = data.get("error", {}).get("message", "unknown reason")
            raise RuntimeError(f"Meta access token is invalid: {error_msg}")

        expires_at = data.get("expires_at", 0)
        if expires_at:
            expiry_dt = datetime.fromtimestamp(expires_at, tz=timezone.utc)
            days_remaining = (expiry_dt - datetime.now(timezone.utc)).days
            logger.info(f"Meta token valid. Expires: {expiry_dt.date()} ({days_remaining}d)")
            if days_remaining < 7:
                logger.warning(f"Meta token expires in {days_remaining} days! Refresh soon.")
        else:
            logger.info("Meta token is valid (never expires)")

        return data

    def paginate(self, url: str, params: dict) -> list[dict]:
        """Fetch all pages via Meta cursor pagination."""
        all_data = []
        while url:
            resp = self._session.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            all_data.extend(data.get("data", []))
            url = data.get("paging", {}).get("next")
            params = None  # next URL has params baked in
        return all_data

    def paginate_creatives(self, url: str, params: dict) -> list[dict]:
        """Paginate creatives, tolerating 500s caused by individual bad creatives.

        Meta sometimes returns 500 for a page if it contains a creative whose
        ``object_story_spec`` is malformed — and occasionally for any field on
        a specific bad creative. Fallback chain on 500:

        1. Strip ``object_story_spec`` from fields and retry (works on both the
           first page and on cursor URLs where fields are baked in).
        2. Shrink ``limit`` (50 → 10 → 1) to isolate the bad creative; the page
           that contains it is dropped, the others are kept.
        3. If even ``limit=1`` 500s, the cursor itself is poisoned: log and
           return what we have rather than failing the whole pipeline.
        """
        all_data = []
        while url:
            resp = self._session.get(url, params=params, timeout=60)
            if resp.status_code == 500:
                resp = self._recover_from_500(url, params)
                if resp is None:
                    logger.error(
                        "Unrecoverable 500 from Meta creatives at cursor %s; "
                        "returning %d creatives gathered so far",
                        _extract_cursor(url), len(all_data),
                    )
                    return all_data
            resp.raise_for_status()
            data = resp.json()
            all_data.extend(data.get("data", []))
            url = data.get("paging", {}).get("next")
            params = None
        return all_data

    def _recover_from_500(self, url: str, params: dict | None):
        """Attempt to recover a 500 on the creatives endpoint.

        Returns a successful response or ``None`` if all attempts fail.
        """
        # 1. Strip object_story_spec.
        if params and "object_story_spec" in params.get("fields", ""):
            logger.warning("500 on creatives page, retrying without object_story_spec")
            params["fields"] = _FIELDS_BASIC_CREATIVES
            resp = self._session.get(url, params=params, timeout=60)
            if resp.ok:
                return resp
            url = resp.url  # subsequent retries use baked-in URL
            params = None
        elif "object_story_spec" in url:
            logger.warning("500 on paginated creatives, retrying without object_story_spec")
            url = _strip_object_story_spec(url)
            resp = self._session.get(url, timeout=60)
            if resp.ok:
                return resp

        # 2. Shrink limit to skip past the bad creative.
        for new_limit in (10, 1):
            shrunk = _set_limit(url, new_limit)
            logger.warning("Retrying creatives cursor with limit=%d", new_limit)
            resp = self._session.get(shrunk, timeout=60)
            if resp.ok:
                return resp

        return None

    def get_campaigns(self) -> list[dict]:
        url = self._api_url(f"act_{self.account_id}/campaigns")
        return self.paginate(url, {
            "access_token": self.access_token,
            "fields": "id,name,objective,status,daily_budget,lifetime_budget,created_time,updated_time",
            "limit": 500,
        })

    def get_adsets(self) -> list[dict]:
        url = self._api_url(f"act_{self.account_id}/adsets")
        return self.paginate(url, {
            "access_token": self.access_token,
            "fields": (
                "id,name,campaign_id,status,daily_budget,lifetime_budget,"
                "targeting,optimization_goal,billing_event,created_time,updated_time"
            ),
            "limit": 500,
        })

    def get_ads(self) -> list[dict]:
        url = self._api_url(f"act_{self.account_id}/ads")
        return self.paginate(url, {
            "access_token": self.access_token,
            "fields": "id,name,adset_id,campaign_id,status,creative{id},created_time,updated_time",
            "limit": 500,
        })

    def get_creatives(self) -> list[dict]:
        url = self._api_url(f"act_{self.account_id}/adcreatives")
        return self.paginate_creatives(url, {
            "access_token": self.access_token,
            "fields": _FIELDS_FULL_CREATIVES,
            "limit": 50,
        })

    def get_insights(self, start_date: str, end_date: str) -> list[dict]:
        """Fetch daily ad-level insights for a date range."""
        url = self._api_url(f"act_{self.account_id}/insights")
        params = {
            "access_token": self.access_token,
            "fields": (
                "date_start,campaign_id,campaign_name,adset_id,adset_name,"
                "ad_id,ad_name,impressions,clicks,spend,cpc,cpm,ctr,"
                "reach,frequency,actions,action_values"
            ),
            "level": "ad",
            "time_increment": 1,
            "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
            "limit": 500,
        }
        return self.paginate(url, params)

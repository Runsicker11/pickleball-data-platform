"""dlt source for Meta Ads — 5 resources: campaigns, adsets, ads, creatives, daily_insights."""

import logging
from datetime import date, timedelta

import dlt

from .client import MetaAdsClient
from .helpers import (
    extract_actions,
    extract_creative_text,
    now_utc_str,
    safe_float,
    safe_int,
    to_bq_timestamp,
)

logger = logging.getLogger(__name__)


@dlt.source(name="meta_ads")
def meta_ads_source(
    account_id: str = dlt.secrets.value,
    access_token: str = dlt.secrets.value,
    app_id: str = "",
    app_secret: str = "",
    api_version: str = "v21.0",
    days_back: int = 3,
):
    """dlt source yielding 5 Meta Ads resources."""
    client = MetaAdsClient(
        account_id=account_id,
        access_token=access_token,
        app_id=app_id,
        app_secret=app_secret,
        api_version=api_version,
    )

    # Validate token on startup
    if app_id and app_secret:
        client.validate_token()

    end_date = date.today() - timedelta(days=1)  # yesterday (today incomplete)
    start_date = end_date - timedelta(days=days_back - 1)

    yield _campaigns_resource(client)
    yield _adsets_resource(client)
    yield _ads_resource(client)
    yield _creatives_resource(client)
    yield _daily_insights_resource(client, start_date, end_date)


def _campaigns_resource(client: MetaAdsClient):
    @dlt.resource(name="campaigns", write_disposition="replace")
    def campaigns():
        now_str = now_utc_str()
        for c in client.get_campaigns():
            yield {
                "campaign_id": c["id"],
                "campaign_name": c.get("name"),
                "objective": c.get("objective"),
                "status": c.get("status"),
                "daily_budget": safe_float(c.get("daily_budget")),
                "lifetime_budget": safe_float(c.get("lifetime_budget")),
                "created_time": to_bq_timestamp(c.get("created_time")),
                "updated_time": to_bq_timestamp(c.get("updated_time")),
                "ingested_at": now_str,
            }
    return campaigns


def _adsets_resource(client: MetaAdsClient):
    @dlt.resource(name="adsets", write_disposition="replace")
    def adsets():
        now_str = now_utc_str()
        for a in client.get_adsets():
            targeting = a.get("targeting", {})
            targeting_summary = str(targeting.get("geo_locations", ""))[:1000] if targeting else None
            yield {
                "adset_id": a["id"],
                "adset_name": a.get("name"),
                "campaign_id": a.get("campaign_id"),
                "status": a.get("status"),
                "daily_budget": safe_float(a.get("daily_budget")),
                "lifetime_budget": safe_float(a.get("lifetime_budget")),
                "targeting_summary": targeting_summary,
                "optimization_goal": a.get("optimization_goal"),
                "billing_event": a.get("billing_event"),
                "created_time": to_bq_timestamp(a.get("created_time")),
                "updated_time": to_bq_timestamp(a.get("updated_time")),
                "ingested_at": now_str,
            }
    return adsets


def _ads_resource(client: MetaAdsClient):
    @dlt.resource(name="ads", write_disposition="replace")
    def ads():
        now_str = now_utc_str()
        for a in client.get_ads():
            creative = a.get("creative", {})
            yield {
                "ad_id": a["id"],
                "ad_name": a.get("name"),
                "adset_id": a.get("adset_id"),
                "campaign_id": a.get("campaign_id"),
                "status": a.get("status"),
                "creative_id": creative.get("id"),
                "created_time": to_bq_timestamp(a.get("created_time")),
                "updated_time": to_bq_timestamp(a.get("updated_time")),
                "ingested_at": now_str,
            }
    return ads


def _creatives_resource(client: MetaAdsClient):
    @dlt.resource(name="creatives", write_disposition="replace")
    def creatives():
        now_str = now_utc_str()
        # Get ads first to link creative_id -> ad_id
        raw_ads = client.get_ads()
        creative_to_ad = {}
        for ad in raw_ads:
            cid = ad.get("creative", {}).get("id")
            if cid:
                creative_to_ad[cid] = {"ad_id": ad["id"], "ad_name": ad.get("name")}

        for c in client.get_creatives():
            creative_id = c["id"]
            ad_info = creative_to_ad.get(creative_id, {})
            text = extract_creative_text(c)
            yield {
                "creative_id": creative_id,
                "ad_id": ad_info.get("ad_id"),
                "ad_name": ad_info.get("ad_name"),
                "title": text["title"],
                "body": text["body"],
                "link_description": text["link_description"],
                "cta_type": text["cta_type"],
                "image_url": text["image_url"],
                "video_id": text["video_id"],
                "thumbnail_url": c.get("thumbnail_url"),
                "object_type": c.get("object_type"),
                "page_id": text["page_id"],
                "instagram_actor_id": text["instagram_actor_id"],
                "created_time": to_bq_timestamp(c.get("created_time")),
                "ingested_at": now_str,
            }
    return creatives


def _daily_insights_resource(client: MetaAdsClient, start_date: date, end_date: date):
    @dlt.resource(
        name="daily_insights",
        write_disposition="merge",
        merge_key=["date_start", "ad_id"],
        primary_key=["date_start", "ad_id"],
    )
    def daily_insights():
        now_str = now_utc_str()
        raw = client.get_insights(start_date.isoformat(), end_date.isoformat())
        for row in raw:
            actions_parsed = extract_actions(
                row.get("actions"), row.get("action_values")
            )
            yield {
                "date_start": row["date_start"],
                "campaign_id": row.get("campaign_id"),
                "campaign_name": row.get("campaign_name"),
                "adset_id": row.get("adset_id"),
                "adset_name": row.get("adset_name"),
                "ad_id": row.get("ad_id"),
                "ad_name": row.get("ad_name"),
                "impressions": safe_int(row.get("impressions")),
                "clicks": safe_int(row.get("clicks")),
                "spend": safe_float(row.get("spend")),
                "cpc": safe_float(row.get("cpc")),
                "cpm": safe_float(row.get("cpm")),
                "ctr": safe_float(row.get("ctr")),
                "reach": safe_int(row.get("reach")),
                "frequency": safe_float(row.get("frequency")),
                **actions_parsed,
                "ingested_at": now_str,
            }
    return daily_insights

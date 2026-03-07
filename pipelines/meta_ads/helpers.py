"""Helper utilities for Meta Ads pipeline — timestamp conversion, action flattening."""

import logging
from datetime import datetime, timezone

from dateutil import parser as dtparser

logger = logging.getLogger(__name__)

# Meta uses multiple action_type names for the same event
ACTION_TYPE_MAP = {
    "purchases": [
        "omni_purchase", "purchase", "offsite_conversion.fb_pixel_purchase",
    ],
    "purchase_value": [
        "omni_purchase", "purchase", "offsite_conversion.fb_pixel_purchase",
    ],
    "add_to_cart": [
        "omni_add_to_cart", "add_to_cart", "offsite_conversion.fb_pixel_add_to_cart",
    ],
    "add_to_cart_value": [
        "omni_add_to_cart", "add_to_cart", "offsite_conversion.fb_pixel_add_to_cart",
    ],
    "initiate_checkout": [
        "omni_initiated_checkout", "initiated_checkout",
        "offsite_conversion.fb_pixel_initiate_checkout",
    ],
    "initiate_checkout_value": [
        "omni_initiated_checkout", "initiated_checkout",
        "offsite_conversion.fb_pixel_initiate_checkout",
    ],
    "landing_page_views": ["landing_page_view"],
    "link_clicks": ["link_click"],
}


def to_bq_timestamp(val: str | None) -> str | None:
    """Convert Meta's ISO timestamp (e.g. 2025-06-23T09:19:47-0600) to BQ format."""
    if not val:
        return None
    try:
        dt = dtparser.isoparse(val).astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def extract_actions(actions: list[dict] | None, action_values: list[dict] | None) -> dict:
    """Parse Meta's nested actions/action_values into flat columns."""
    result = {}

    actions_lookup = {}
    for a in (actions or []):
        actions_lookup[a["action_type"]] = int(float(a["value"]))

    values_lookup = {}
    for av in (action_values or []):
        values_lookup[av["action_type"]] = float(av["value"])

    for col, type_names in ACTION_TYPE_MAP.items():
        is_value = col.endswith("_value")
        lookup = values_lookup if is_value else actions_lookup
        val = None
        for tn in type_names:
            if tn in lookup:
                val = lookup[tn]
                break
        result[col] = val

    return result


def extract_creative_text(creative: dict) -> dict:
    """Extract title/body/CTA from object_story_spec, falling back to top-level fields."""
    title = creative.get("title")
    body = creative.get("body")
    link_description = None
    cta_type = None
    video_id = None
    image_url = creative.get("image_url")
    page_id = None
    instagram_actor_id = None

    oss = creative.get("object_story_spec", {})
    if oss:
        page_id = oss.get("page_id")
        instagram_actor_id = oss.get("instagram_actor_id")

        video_data = oss.get("video_data", {})
        if video_data:
            body = body or video_data.get("message")
            title = title or video_data.get("title")
            link_description = video_data.get("link_description")
            video_id = video_data.get("video_id")
            image_url = image_url or video_data.get("image_url")
            cta = video_data.get("call_to_action", {})
            cta_type = cta.get("type") if cta else None

        link_data = oss.get("link_data", {})
        if link_data and not body:
            body = body or link_data.get("message")
            title = title or link_data.get("name")
            link_description = link_description or link_data.get("description")
            image_url = image_url or link_data.get("image_url") or link_data.get("picture")
            cta = link_data.get("call_to_action", {})
            cta_type = cta_type or (cta.get("type") if cta else None)

        photo_data = oss.get("photo_data", {})
        if photo_data and not body:
            body = body or photo_data.get("message")
            image_url = image_url or photo_data.get("image_url")

    cta_type = cta_type or creative.get("call_to_action_type")

    return {
        "title": title,
        "body": body,
        "link_description": link_description,
        "cta_type": cta_type,
        "video_id": video_id,
        "image_url": image_url,
        "page_id": page_id,
        "instagram_actor_id": instagram_actor_id,
    }


def safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

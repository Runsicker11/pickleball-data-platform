"""Helper utilities for Shopify pipeline — UTM parsing, GID conversion, row normalization."""

import logging
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)

UTM_PARAMS = ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"]


def parse_utms(landing_site: str | None) -> dict:
    """Extract UTM parameters from a Shopify landing_site URL."""
    result = {p: None for p in UTM_PARAMS}
    if not landing_site:
        return result
    try:
        parsed = urlparse(landing_site)
        params = parse_qs(parsed.query)
        for p in UTM_PARAMS:
            values = params.get(p)
            if values:
                result[p] = values[0]
    except Exception:
        pass
    return result


def gid_to_int(gid: str) -> int:
    """Convert Shopify GID (e.g. 'gid://shopify/Product/123') to integer."""
    return int(gid.rsplit("/", 1)[-1])


def parse_link_header(link_header: str | None) -> str | None:
    """Extract the 'next' page URL from Shopify's Link header."""
    if not link_header:
        return None
    for part in link_header.split(","):
        if 'rel="next"' in part:
            url = part.split(";")[0].strip().strip("<>")
            return url
    return None


def safe_float(val) -> float | None:
    """Safely convert a value to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def now_utc_str() -> str:
    """Return current UTC time as YYYY-MM-DD HH:MM:SS string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

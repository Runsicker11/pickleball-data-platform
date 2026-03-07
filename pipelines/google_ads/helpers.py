"""Helper utilities for Google Ads pipeline — micros conversion, type handling."""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def micros_to_dollars(micros: int | float | None) -> float | None:
    """Convert Google Ads micros to USD (divide by 1,000,000)."""
    if micros is None or micros == 0:
        return None
    return micros / 1_000_000


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

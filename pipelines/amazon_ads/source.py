"""
dlt source for Amazon Ads — 5 report resources with merge write disposition.
"""

import logging
from datetime import date, timedelta

import dlt

from .client import AmazonAdsClient
from .helpers import normalize_row
from .report_configs import (
    ALL_REPORTS,
    ReportConfig,
)

logger = logging.getLogger(__name__)


def _make_resource(
    client: AmazonAdsClient,
    profile_ids: list[str],
    config: ReportConfig,
    start_date: str,
    end_date: str,
):
    """Factory that builds a dlt resource for a given report config."""

    @dlt.resource(
        name=config.name,
        write_disposition="merge",
        merge_key=config.merge_keys,
        primary_key=config.merge_keys + ["profile_id"],
    )
    def _resource():
        for profile_id in profile_ids:
            logger.info(
                f"[{config.name}] Fetching profile {profile_id}: {start_date}→{end_date}"
            )
            rows = client.fetch_report(
                profile_id=profile_id,
                ad_product=config.ad_product,
                report_type_id=config.report_type_id,
                start_date=start_date,
                end_date=end_date,
                columns=config.columns,
                group_by=config.group_by,
            )
            for row in rows:
                yield normalize_row(row, profile_id)

    return _resource


@dlt.source(name="amazon_ads")
def amazon_ads_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    profile_ids: list[str] = dlt.config.value,
    days_back: int = 7,
    poll_interval: int = 120,
    poll_timeout_minutes: int = 45,
    reports: list[ReportConfig] | None = None,
):
    """dlt source that yields 5 Amazon Ads report resources.

    Args:
        client_id: Amazon LWA client ID
        client_secret: Amazon LWA client secret
        refresh_token: Amazon LWA refresh token
        profile_ids: List of Amazon Ads profile IDs to pull
        days_back: Number of days of history to pull
        poll_interval: Seconds between report status polls
        poll_timeout_minutes: Max minutes to wait for a report
        reports: Optional subset of ReportConfig objects; defaults to ALL_REPORTS
    """
    if reports is None:
        reports = ALL_REPORTS

    end_date = date.today().isoformat()
    start_date = (date.today() - timedelta(days=days_back)).isoformat()

    client = AmazonAdsClient(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        poll_interval=poll_interval,
        poll_timeout_minutes=poll_timeout_minutes,
    )

    for report_config in reports:
        yield _make_resource(client, profile_ids, report_config, start_date, end_date)

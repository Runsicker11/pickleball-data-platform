"""dlt source for Search Console — 1 multi-site resource: performance."""

import logging
from datetime import date, datetime, timedelta, timezone

import dlt

from .client import SearchConsoleClient

logger = logging.getLogger(__name__)


@dlt.source(name="search_console")
def search_console_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    site_url: str = "",
    site_url_shop: str = "",
    days_back: int = 7,
):
    """dlt source yielding 1 multi-site Search Console resource."""
    client = SearchConsoleClient(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        site_url=site_url,
        site_url_shop=site_url_shop,
    )
    client.validate_access()

    # SC data lags 2-3 days
    end_date = date.today() - timedelta(days=3)
    start_date = end_date - timedelta(days=days_back - 1)

    yield _performance_resource(client, start_date, end_date)


def _performance_resource(client: SearchConsoleClient, start_date: date, end_date: date):
    @dlt.resource(
        name="performance",
        write_disposition="merge",
        merge_key=["query_date", "site", "query", "page", "country", "device"],
        primary_key=["query_date", "site", "query", "page", "country", "device"],
    )
    def performance():
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        for site_url in client.site_urls:
            site = client.site_label(site_url)
            raw = client.query_performance(
                site_url, start_date.isoformat(), end_date.isoformat()
            )

            for row in raw:
                keys = row.get("keys", [])
                if len(keys) < 5:
                    continue

                yield {
                    "query_date": keys[0],
                    "site": site,
                    "query": keys[1],
                    "page": keys[2],
                    "country": keys[3],
                    "device": keys[4],
                    "impressions": int(row.get("impressions", 0)),
                    "clicks": int(row.get("clicks", 0)),
                    "ctr": round(row.get("ctr", 0.0), 6),
                    "position": round(row.get("position", 0.0), 2),
                    "ingested_at": now_str,
                }

            logger.info(
                f"[{site}] Total rows: {len(raw)} ({start_date} to {end_date})"
            )

    return performance

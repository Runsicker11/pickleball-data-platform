"""dlt source for Google Trends — 2 resources: interest_over_time, related_queries."""

import logging
import time
from datetime import datetime, timezone

import dlt

logger = logging.getLogger(__name__)

KEYWORDS = [
    "pickleball tape",
    "tungsten tape pickleball",
    "pickleball edge guard",
    "pickleball grip tape",
    "pickleball accessories",
    "pickleball paddle",
]

# pytrends supports max 5 keywords per payload
_BATCH_SIZE = 5


@dlt.source(name="google_trends")
def google_trends_source():
    """dlt source yielding Google Trends interest and related query data."""
    yield _interest_over_time_resource()
    yield _related_queries_resource()


def _interest_over_time_resource():
    @dlt.resource(
        name="interest_over_time",
        write_disposition="replace",
    )
    def interest_over_time():
        from pytrends.request import TrendReq

        pytrends = TrendReq(hl="en-US", tz=360)
        ingested = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        batches = [KEYWORDS[i : i + _BATCH_SIZE] for i in range(0, len(KEYWORDS), _BATCH_SIZE)]

        for batch in batches:
            try:
                pytrends.build_payload(batch, timeframe="today 5-y", geo="US")
                df = pytrends.interest_over_time()
            except Exception as exc:
                logger.warning(f"Failed to fetch interest_over_time for batch {batch}: {exc}")
                time.sleep(2)
                continue

            if df.empty:
                time.sleep(1)
                continue

            is_partial_col = "isPartial" in df.columns

            for kw in batch:
                if kw not in df.columns:
                    continue
                for ts, score in df[kw].items():
                    is_partial = bool(df.loc[ts, "isPartial"]) if is_partial_col else False
                    yield {
                        "week": ts.date().isoformat(),
                        "keyword": kw,
                        "interest_score": int(score),
                        "is_partial": is_partial,
                        "ingested_at": ingested,
                    }

            time.sleep(1)

        logger.info(f"interest_over_time: yielded data for {len(KEYWORDS)} keywords")

    return interest_over_time


def _related_queries_resource():
    @dlt.resource(
        name="related_queries",
        write_disposition="replace",
    )
    def related_queries():
        from pytrends.request import TrendReq

        pytrends = TrendReq(hl="en-US", tz=360)
        ingested = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        for kw in KEYWORDS:
            try:
                pytrends.build_payload([kw], timeframe="today 5-y", geo="US")
                result = pytrends.related_queries()
            except Exception as exc:
                logger.warning(f"Failed to fetch related_queries for '{kw}': {exc}")
                time.sleep(2)
                continue

            kw_result = result.get(kw, {})
            for query_type in ("top", "rising"):
                df = kw_result.get(query_type)
                if df is None or df.empty:
                    continue
                for _, row in df.iterrows():
                    yield {
                        "keyword": kw,
                        "query_type": query_type,
                        "related_query": row.get("query", ""),
                        "value": int(row.get("value", 0)),
                        "ingested_at": ingested,
                    }

            time.sleep(1)

        logger.info(f"related_queries: yielded data for {len(KEYWORDS)} keywords")

    return related_queries

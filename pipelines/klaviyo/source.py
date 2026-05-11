"""dlt source for Klaviyo — 6 resources: campaigns, campaign_metrics, flows, flow_messages, metrics_timeline, profiles."""

import json
import logging
from datetime import date, datetime, timedelta, timezone

import dlt

from .client import KlaviyoClient


def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

logger = logging.getLogger(__name__)

# Klaviyo metrics to pull for metrics_timeline.
# Each tuple is (metric_name, preferred_integration). Klaviyo accounts often have
# duplicate metric names across integrations (e.g. both Shopify and WooCommerce
# expose "Placed Order"); preferred_integration disambiguates. None means the
# metric is Klaviyo-native and only appears once.
_TARGET_METRICS: tuple[tuple[str, str | None], ...] = (
    ("Opened Email", None),
    ("Clicked Email", None),
    ("Placed Order", "Shopify"),
)


@dlt.source(name="klaviyo")
def klaviyo_source(
    api_key: str = dlt.secrets.value,
    days_back: int = 7,
):
    """dlt source yielding 6 Klaviyo resources."""
    client = KlaviyoClient(api_key=api_key)

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back - 1)

    # Resolve metric IDs once and share across resources that need them.
    # campaign_metrics requires the Shopify "Placed Order" ID as the
    # conversion_metric_id for campaign-values-reports.
    metric_map = _get_metric_map(client, _TARGET_METRICS)
    placed_order_id = metric_map.get("Placed Order")

    yield _campaigns_resource(client)
    yield _campaign_metrics_resource(client, placed_order_id)
    yield _flows_resource(client)
    yield _flow_messages_resource(client)
    yield _metrics_timeline_resource(client, metric_map, start_date, end_date)
    yield _profiles_resource(client, start_date)


def _campaigns_resource(client: KlaviyoClient):
    @dlt.resource(name="campaigns", write_disposition="replace")
    def campaigns():
        ingested = _now_utc_str()
        params = {
            "filter": "equals(messages.channel,'email')",
        }
        for item in client.paginate("/campaigns/", params=params):
            attrs = item.get("attributes") or {}
            yield {
                "id": item.get("id"),
                "name": attrs.get("name"),
                "status": attrs.get("status"),
                "send_time": attrs.get("send_time"),
                "created_at": attrs.get("created_at"),
                "updated_at": attrs.get("updated_at"),
                "ingested_at": ingested,
            }

    return campaigns


def _campaign_metrics_resource(client: KlaviyoClient, placed_order_metric_id: str | None):
    @dlt.resource(
        name="campaign_metrics",
        write_disposition="merge",
        merge_key=["campaign_id"],
        primary_key=["campaign_id"],
    )
    def campaign_metrics():
        ingested = _now_utc_str()

        if not placed_order_metric_id:
            logger.warning(
                "Skipping campaign_metrics: no Shopify Placed Order metric ID discovered"
            )
            return

        # campaign-values-reports returns all per-campaign stats in one call.
        # Use a wide timeframe to cover the full account history.
        payload = {
            "data": {
                "type": "campaign-values-report",
                "attributes": {
                    "timeframe": {"key": "last_12_months"},
                    "conversion_metric_id": placed_order_metric_id,
                    "statistics": [
                        "opens", "clicks", "unsubscribes",
                        "delivered", "recipients", "conversion_value", "conversions",
                    ],
                },
            }
        }
        result = client.post("/campaign-values-reports/", payload)
        rows = (result.get("data") or {}).get("attributes", {}).get("results", [])

        # Build a name lookup from the campaigns list
        campaigns_list = list(client.paginate("/campaigns/", params={"filter": "equals(messages.channel,'email')"}))
        name_map = {c["id"]: (c.get("attributes") or {}).get("name") for c in campaigns_list}
        send_time_map = {c["id"]: (c.get("attributes") or {}).get("send_time") for c in campaigns_list}

        for row in rows:
            groupings = row.get("groupings") or {}
            stats = row.get("statistics") or {}
            campaign_id = groupings.get("campaign_id")
            send_time = send_time_map.get(campaign_id, "")
            yield {
                "campaign_id": campaign_id,
                "campaign_name": name_map.get(campaign_id),
                "send_date": send_time[:10] if send_time else None,
                "recipients": stats.get("recipients"),
                "delivered": stats.get("delivered"),
                "opens": stats.get("opens"),
                "clicks": stats.get("clicks"),
                "unsubscribes": stats.get("unsubscribes"),
                "conversions": stats.get("conversions"),
                "revenue": stats.get("conversion_value"),
                "ingested_at": ingested,
            }

    return campaign_metrics


def _flows_resource(client: KlaviyoClient):
    @dlt.resource(name="flows", write_disposition="replace")
    def flows():
        ingested = _now_utc_str()
        params = {}
        for item in client.paginate("/flows/", params=params):
            attrs = item.get("attributes") or {}
            yield {
                "id": item.get("id"),
                "name": attrs.get("name"),
                "status": attrs.get("status"),
                "trigger_type": attrs.get("trigger_type"),
                "created": attrs.get("created"),
                "updated": attrs.get("updated"),
                "ingested_at": ingested,
            }

    return flows


def _flow_messages_resource(client: KlaviyoClient):
    @dlt.resource(name="flow_messages", write_disposition="replace")
    def flow_messages():
        ingested = _now_utc_str()
        # Flow messages in Klaviyo v3 are nested under flow actions, not directly listable.
        # Enumerate via flows → actions → messages.
        for flow_item in client.paginate("/flows/"):
            flow_id = flow_item.get("id")
            try:
                for action in client.paginate(f"/flows/{flow_id}/flow-actions/"):
                    action_id = action.get("id")
                    try:
                        for msg in client.paginate(f"/flow-actions/{action_id}/flow-messages/"):
                            attrs = msg.get("attributes") or {}
                            yield {
                                "id": msg.get("id"),
                                "flow_id": flow_id,
                                "action_id": action_id,
                                "name": attrs.get("name"),
                                "status": attrs.get("status"),
                                "channel": attrs.get("channel"),
                                "created_at": attrs.get("created_at"),
                                "updated_at": attrs.get("updated_at"),
                                "ingested_at": ingested,
                            }
                    except Exception as exc:
                        logger.warning("Could not get messages for action %s: %s", action_id, exc)
            except Exception as exc:
                logger.warning("Could not get actions for flow %s: %s", flow_id, exc)

    return flow_messages


def _metrics_timeline_resource(
    client: KlaviyoClient,
    metric_map: dict[str, str],
    start_date: date,
    end_date: date,
):
    @dlt.resource(
        name="metrics_timeline",
        write_disposition="merge",
        merge_key=["date", "metric_name"],
        primary_key=["date", "metric_name"],
    )
    def metrics_timeline():
        ingested = _now_utc_str()

        for metric_name, metric_id in metric_map.items():
            try:
                payload = {
                    "data": {
                        "type": "metric-aggregate",
                        "attributes": {
                            "metric_id": metric_id,
                            "measurements": ["count", "sum_value"],
                            "interval": "day",
                            "filter": (
                                f"greater-or-equal(datetime,{start_date.isoformat()}T00:00:00+00:00),"
                                f"less-than(datetime,{end_date.isoformat()}T23:59:59+00:00)"
                            ),
                        },
                    }
                }
                result = client.post("/metric-aggregates/", payload)
                attrs = (result.get("data") or {}).get("attributes") or {}
                dates = attrs.get("dates") or []
                rows = attrs.get("data") or []
                measurements = (rows[0].get("measurements") or {}) if rows else {}
                counts = measurements.get("count") or []
                sums = measurements.get("sum_value") or []

                for i, day_str in enumerate(dates):
                    yield {
                        "date": day_str[:10],
                        "metric_id": metric_id,
                        "metric_name": metric_name,
                        "value": counts[i] if i < len(counts) else None,
                        "sum_value": sums[i] if i < len(sums) else None,
                        "ingested_at": ingested,
                    }
            except Exception as exc:
                logger.warning("Could not get metrics_timeline for %s: %s", metric_name, exc)

    return metrics_timeline


def _profiles_resource(client: KlaviyoClient, updated_since: date):
    @dlt.resource(
        name="profiles",
        write_disposition="merge",
        primary_key="id",
    )
    def profiles():
        ingested = _now_utc_str()
        params = {
            "page[size]": 100,
            "additional-fields[profile]": "subscriptions",
            "filter": f"greater-than(updated,{(updated_since - timedelta(days=1)).isoformat()}T00:00:00+00:00)",
            "sort": "updated",
        }
        for item in client.paginate("/profiles/", params=params):
            attrs = item.get("attributes") or {}
            subs = attrs.get("subscriptions") or {}
            email_mkt = (subs.get("email") or {}).get("marketing") or {}
            sms_mkt = (subs.get("sms") or {}).get("marketing") or {}
            location = attrs.get("location") or {}
            # properties is a free-form dict — serialize to JSON string so BQ
            # can store it; Grapevine survey answers land here as custom keys
            raw_props = attrs.get("properties") or {}
            yield {
                "id": item.get("id"),
                "email": attrs.get("email"),
                "phone_number": attrs.get("phone_number"),
                "first_name": attrs.get("first_name"),
                "last_name": attrs.get("last_name"),
                "created": attrs.get("created"),
                "updated": attrs.get("updated"),
                # Flattened subscription status
                "email_consent": email_mkt.get("consent"),
                "email_subscribed": email_mkt.get("can_receive_email_marketing"),
                "email_consent_timestamp": email_mkt.get("consent_timestamp"),
                "sms_consent": sms_mkt.get("consent"),
                "sms_subscribed": sms_mkt.get("can_receive_sms_marketing"),
                # Location for geo segmentation
                "city": location.get("city"),
                "region": location.get("region"),
                "country": location.get("country"),
                # Raw custom properties (includes Grapevine survey answers, signup source, etc.)
                "properties": json.dumps(raw_props) if raw_props else None,
                "ingested_at": ingested,
            }

    return profiles


def _get_metric_map(
    client: KlaviyoClient,
    targets: tuple[tuple[str, str | None], ...],
) -> dict[str, str]:
    """Return {metric_name: metric_id} for the requested (name, integration) targets.

    When integration is None, the first metric matching the name wins. When set,
    we require integration.name to match — protects against duplicate metric names
    across integrations (e.g. Shopify vs WooCommerce "Placed Order").
    """
    by_name_integration: dict[tuple[str, str | None], str] = {}
    try:
        for item in client.paginate(
            "/metrics/", params={"fields[metric]": "name,integration"}
        ):
            attrs = item.get("attributes") or {}
            name = attrs.get("name", "")
            integration = (attrs.get("integration") or {}).get("name")
            for target_name, target_integration in targets:
                if name != target_name:
                    continue
                if target_integration is not None and integration != target_integration:
                    continue
                key = (target_name, target_integration)
                # First match wins (API returns one row per metric)
                by_name_integration.setdefault(key, item["id"])
    except Exception as exc:
        logger.warning("Could not fetch Klaviyo metrics list: %s", exc)

    metric_map: dict[str, str] = {}
    for target_name, target_integration in targets:
        mid = by_name_integration.get((target_name, target_integration))
        if mid is None:
            logger.warning(
                "Klaviyo metric not found: name=%s integration=%s",
                target_name,
                target_integration,
            )
            continue
        metric_map[target_name] = mid
    return metric_map

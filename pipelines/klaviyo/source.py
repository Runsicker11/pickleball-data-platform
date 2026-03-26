"""dlt source for Klaviyo — 5 resources: campaigns, campaign_metrics, flows, flow_messages, metrics_timeline."""

import logging
from datetime import date, datetime, timedelta, timezone

import dlt

from .client import KlaviyoClient


def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

logger = logging.getLogger(__name__)

# Klaviyo metric names to pull for metrics_timeline
_TARGET_METRICS = {"Opened Email", "Clicked Email", "Placed Order"}


@dlt.source(name="klaviyo")
def klaviyo_source(
    api_key: str = dlt.secrets.value,
    days_back: int = 7,
):
    """dlt source yielding 5 Klaviyo resources."""
    client = KlaviyoClient(api_key=api_key)

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back - 1)
    start_date_90d = date.today() - timedelta(days=90)

    yield _campaigns_resource(client)
    yield _campaign_metrics_resource(client)
    yield _flows_resource(client)
    yield _flow_messages_resource(client)
    yield _metrics_timeline_resource(client, start_date_90d if days_back >= 90 else start_date, end_date)


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


def _campaign_metrics_resource(client: KlaviyoClient):
    @dlt.resource(
        name="campaign_metrics",
        write_disposition="merge",
        merge_key=["campaign_id"],
        primary_key=["campaign_id"],
        columns={
            "recipients": {"data_type": "bigint", "nullable": True},
            "opens": {"data_type": "double", "nullable": True},
            "clicks": {"data_type": "double", "nullable": True},
            "revenue": {"data_type": "double", "nullable": True},
            "unsubscribes": {"data_type": "double", "nullable": True},
        },
    )
    def campaign_metrics():
        ingested = _now_utc_str()

        # Pull all sent campaigns
        params = {
            "filter": "and(equals(messages.channel,'email'),equals(status,'Sent'))",
        }
        sent_campaigns = list(client.paginate("/campaigns/", params=params))

        # Get metric IDs for opens, clicks, revenue, unsubscribes
        metric_map = _get_metric_map(client, {"Opened Email", "Clicked Email", "Placed Order", "Unsubscribed"})

        for item in sent_campaigns:
            campaign_id = item.get("id")
            attrs = item.get("attributes") or {}
            campaign_name = attrs.get("name")
            send_time = attrs.get("send_time")

            # Recipient count
            recipients = None
            try:
                est = client.get(f"/campaign-recipient-estimation/{campaign_id}/")
                recipients = (est.get("data") or {}).get("attributes", {}).get("estimated_recipient_count")
            except Exception as exc:
                logger.warning("Could not get recipients for campaign %s: %s", campaign_id, exc)

            # Aggregate metrics via metric-aggregates endpoint
            metric_totals: dict[str, float] = {}
            send_date_str = (send_time or "")[:10]
            if send_date_str:
                for metric_name, metric_id in metric_map.items():
                    try:
                        payload = {
                            "data": {
                                "type": "metric-aggregate",
                                "attributes": {
                                    "metric_id": metric_id,
                                    "measurements": ["count", "sum_value"],
                                    "interval": "month",
                                    "filter": (
                                        f"greater-or-equal(datetime,{send_date_str}T00:00:00+00:00),"
                                        f"less-than(datetime,{date.today().isoformat()}T23:59:59+00:00),"
                                        f"equals(attributed_message.parent_id,\"{campaign_id}\")"
                                    ),
                                },
                            }
                        }
                        result = client.post("/metric-aggregates/", payload)
                        rows = (result.get("data") or {}).get("attributes", {}).get("data", [])
                        counts = (rows[0].get("measurements", {}).get("count", []) if rows else [])
                        metric_totals[metric_name] = sum(v for v in counts if v is not None)

                    except Exception as exc:
                        logger.warning("Could not get %s for campaign %s: %s", metric_name, campaign_id, exc)

            yield {
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "send_date": send_date_str or None,
                "recipients": recipients,
                "opens": metric_totals.get("Opened Email"),
                "clicks": metric_totals.get("Clicked Email"),
                "revenue": metric_totals.get("Placed Order"),
                "unsubscribes": metric_totals.get("Unsubscribed"),
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


def _metrics_timeline_resource(client: KlaviyoClient, start_date: date, end_date: date):
    @dlt.resource(
        name="metrics_timeline",
        write_disposition="merge",
        merge_key=["date", "metric_name"],
        primary_key=["date", "metric_name"],
    )
    def metrics_timeline():
        ingested = _now_utc_str()
        metric_map = _get_metric_map(client, _TARGET_METRICS)

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
                counts = (rows[0].get("measurements", {}).get("count", []) if rows else [])

                for i, day_str in enumerate(dates):
                    yield {
                        "date": day_str[:10],
                        "metric_id": metric_id,
                        "metric_name": metric_name,
                        "value": counts[i] if i < len(counts) else None,
                        "ingested_at": ingested,
                    }
            except Exception as exc:
                logger.warning("Could not get metrics_timeline for %s: %s", metric_name, exc)

    return metrics_timeline


def _get_metric_map(client: KlaviyoClient, target_names: set[str]) -> dict[str, str]:
    """Return {metric_name: metric_id} for the requested metric names."""
    metric_map: dict[str, str] = {}
    try:
        for item in client.paginate("/metrics/", params={"fields[metric]": "name"}):
            name = (item.get("attributes") or {}).get("name", "")
            if name in target_names:
                metric_map[name] = item["id"]
            if len(metric_map) == len(target_names):
                break
    except Exception as exc:
        logger.warning("Could not fetch Klaviyo metrics list: %s", exc)
    return metric_map

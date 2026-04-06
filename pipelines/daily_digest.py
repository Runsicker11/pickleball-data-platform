"""Daily pipeline health digest — queries BigQuery ops.pipeline_runs and posts
a failure recap + 30-day trend to Slack.

Usage:
    python -m pipelines.daily_digest
"""

import logging
import sys
from datetime import date, datetime, timezone

from google.cloud import bigquery

from .config import GCP_PROJECT_ID
from .notify import send_slack

logger = logging.getLogger(__name__)

FULL_TABLE_ID = f"{GCP_PROJECT_ID}.ops.pipeline_runs"


def _query_yesterday_runs(client: bigquery.Client) -> list[dict]:
    """Return the latest run per pipeline from the past 24 hours."""
    query = f"""
        SELECT
            pipeline_name,
            status,
            rows_loaded,
            ROUND(duration_seconds, 1) AS duration_seconds,
            error_message,
            started_at
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY pipeline_name
                    ORDER BY started_at DESC
                ) AS rn
            FROM `{FULL_TABLE_ID}`
            WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
              AND pipeline_name != 'dbt'
        )
        WHERE rn = 1
        ORDER BY pipeline_name
    """
    rows = list(client.query(query).result())
    return [dict(r) for r in rows]


def _query_30day_trend(client: bigquery.Client) -> list[dict]:
    """Return per-pipeline run counts and success rates over the past 30 days."""
    query = f"""
        SELECT
            pipeline_name,
            COUNT(*) AS total_runs,
            COUNTIF(status = 'success') AS successes,
            COUNTIF(status = 'error') AS failures,
            ROUND(100.0 * COUNTIF(status = 'success') / COUNT(*), 0) AS success_rate
        FROM `{FULL_TABLE_ID}`
        WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
          AND pipeline_name != 'dbt'
        GROUP BY pipeline_name
        ORDER BY success_rate ASC, failures DESC
    """
    rows = list(client.query(query).result())
    return [dict(r) for r in rows]


def _query_dbt_trend(client: bigquery.Client) -> dict | None:
    """Return dbt success rate over the past 30 days."""
    query = f"""
        SELECT
            COUNT(*) AS total_runs,
            COUNTIF(status = 'success') AS successes,
            ROUND(100.0 * COUNTIF(status = 'success') / COUNT(*), 0) AS success_rate
        FROM `{FULL_TABLE_ID}`
        WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
          AND pipeline_name = 'dbt'
    """
    rows = list(client.query(query).result())
    if rows and rows[0]["total_runs"]:
        return dict(rows[0])
    return None


def format_digest(
    yesterday: list[dict],
    trend: list[dict],
    dbt_trend: dict | None,
    report_date: date,
) -> str:
    """Format the daily digest Slack message."""
    lines = []
    date_str = report_date.strftime("%B %-d, %Y") if sys.platform != "win32" else report_date.strftime("%B %d, %Y").replace(" 0", " ")

    failures = [r for r in yesterday if r["status"] == "error"]
    failure_count = len(failures)
    all_ok = failure_count == 0

    header_icon = ":white_check_mark:" if all_ok else ":x:"
    lines.append(f":bar_chart: *Pipeline Health Digest — {date_str}*")
    lines.append("")

    # Yesterday's results
    if not yesterday:
        lines.append("_No pipeline runs recorded in the past 24 hours._")
    elif all_ok:
        lines.append(f"{header_icon} *Yesterday: All {len(yesterday)} pipelines succeeded*")
    else:
        lines.append(f"{header_icon} *Yesterday: {failure_count} failure{'s' if failure_count != 1 else ''}*")
        for r in failures:
            err = (r.get("error_message") or "unknown error")[:120]
            lines.append(f"  :x: *{r['pipeline_name']}* — {err}")

    lines.append("")

    # 30-day trend table
    if trend:
        lines.append("*30-Day Success Rates:*")
        lines.append("```")
        lines.append(f"{'Pipeline':<20} {'Runs':>5} {'✓':>5} {'✗':>5} {'Rate':>6}")
        lines.append("-" * 45)
        for r in trend:
            rate = int(r["success_rate"] or 0)
            rate_str = f"{rate}%"
            flag = " :warning:" if rate < 80 else ""
            lines.append(
                f"{r['pipeline_name']:<20} {int(r['total_runs']):>5} "
                f"{int(r['successes']):>5} {int(r['failures']):>5} {rate_str:>6}{flag}"
            )
        lines.append("```")

    # dbt
    if dbt_trend:
        dbt_rate = int(dbt_trend["success_rate"] or 0)
        dbt_icon = ":white_check_mark:" if dbt_rate >= 80 else ":warning:"
        lines.append(f"{dbt_icon} *dbt* (30-day): {dbt_rate}% success rate ({int(dbt_trend['successes'])}/{int(dbt_trend['total_runs'])} runs)")

    return "\n".join(lines)


def run_digest() -> bool:
    """Query BigQuery and send the daily digest to Slack.

    Returns True on success, False on failure. Never raises.
    """
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        yesterday = _query_yesterday_runs(client)
        trend = _query_30day_trend(client)
        dbt_trend = _query_dbt_trend(client)
    except Exception:
        logger.exception("Failed to query BigQuery for daily digest")
        return False

    today = datetime.now(timezone.utc).date()
    msg = format_digest(yesterday, trend, dbt_trend, today)
    return send_slack(msg)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    success = run_digest()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

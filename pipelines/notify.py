"""Slack webhook notifications for pipeline runs.

Usage:
    from pipelines.notify import send_slack
    send_slack("Pipeline complete!", webhook_url="https://hooks.slack.com/...")
"""

import json
import logging
import os

import requests

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def send_slack(
    message: str,
    webhook_url: str | None = None,
    blocks: list | None = None,
) -> bool:
    """Post a message to Slack via incoming webhook.

    Never raises — wraps all errors so it can't break the pipeline.
    Returns True on success, False on failure.
    """
    url = webhook_url or SLACK_WEBHOOK_URL
    if not url:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping notification")
        return False

    payload: dict = {}
    if blocks:
        payload["blocks"] = blocks
        # Slack requires text as fallback for notifications
        payload["text"] = message
    else:
        payload["text"] = message

    try:
        resp = requests.post(
            url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(f"Slack webhook returned {resp.status_code}: {resp.text}")
            return False
        return True
    except Exception:
        logger.exception("Failed to send Slack notification")
        return False


def format_pipeline_summary(results: dict, dbt_ok: bool, duration_seconds: float) -> str:
    """Format a pipeline run summary for Slack.

    Args:
        results: {pipeline_name: {status, rows, duration, error}}
        dbt_ok: Whether dbt run + test passed
        duration_seconds: Total wall-clock time
    """
    mins = duration_seconds / 60
    all_ok = all(r["status"] == "success" for r in results.values()) and dbt_ok

    icon = ":white_check_mark:" if all_ok else ":x:"
    header = f"{icon} *Daily Pipeline Refresh* ({mins:.1f} min)"

    lines = [header, ""]
    for name, info in results.items():
        if info["status"] == "success":
            lines.append(f":white_check_mark: *{name}*: {info['rows']} rows ({info['duration']:.0f}s)")
        elif info["status"] == "skipped":
            lines.append(f":fast_forward: *{name}*: skipped")
        else:
            lines.append(f":x: *{name}*: FAILED — {info.get('error', 'unknown')}")

    lines.append("")
    dbt_icon = ":white_check_mark:" if dbt_ok else ":x:"
    lines.append(f"{dbt_icon} *dbt run + test*")

    return "\n".join(lines)


def format_token_warning(token_name: str, days_remaining: int) -> str:
    """Format a token expiry warning for Slack."""
    return (
        f":warning: *Token Expiry Warning*\n"
        f"`{token_name}` expires in *{days_remaining} days*. "
        f"Refresh it before it breaks the pipeline."
    )

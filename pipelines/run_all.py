"""Master orchestrator: runs all pipelines + dbt with error isolation.

Usage:
    python -m pipelines.run_all [--days 3] [--skip shopify,meta-ads] [--no-dbt] [--no-slack]

Each pipeline runs in isolation — if one fails, the rest still run.
Results are logged to BigQuery ops.pipeline_runs and a Slack summary is sent.
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

from .health import log_run
from .notify import format_pipeline_summary, format_token_warning, send_slack

logger = logging.getLogger(__name__)

# Pipeline registry: name → (module path, default days, default dataset)
PIPELINES = {
    "shopify": ("pipelines.shopify.pipeline", 3, "raw_shopify"),
    "meta-ads": ("pipelines.meta_ads.pipeline", 3, "raw_meta"),
    "google-ads": ("pipelines.google_ads.pipeline", 3, "raw_google_ads"),
    "search-console": ("pipelines.search_console.pipeline", 7, "raw_search_console"),
    "amazon-ads": ("pipelines.amazon_ads.pipeline", 7, "raw_amazon"),
    "amazon-seller": ("pipelines.amazon_seller.pipeline", 30, "raw_amazon"),
    "quickbooks": ("pipelines.quickbooks.pipeline", 90, "raw_quickbooks"),
    "paypal": ("pipelines.paypal.pipeline", 365, "raw_paypal"),
    "klaviyo": ("pipelines.klaviyo.pipeline", 7, "raw_klaviyo"),
}


def _extract_row_count(load_info) -> int:
    """Extract total rows loaded from dlt LoadInfo object."""
    try:
        metrics = load_info.metrics[load_info.loads_ids[0]]
        total = 0
        for job_metrics in metrics:
            for table_name, table_metrics in job_metrics.items():
                if table_name == "started_at":
                    continue
                if isinstance(table_metrics, dict) and "items_count" in table_metrics:
                    total += table_metrics["items_count"]
        return total
    except Exception:
        return 0


def _check_meta_token_expiry() -> None:
    """Warn via Slack if Meta access token is near expiry."""
    expires_str = os.getenv("META_TOKEN_EXPIRES", "")
    if not expires_str:
        return

    try:
        from datetime import date

        expires = date.fromisoformat(expires_str)
        days_remaining = (expires - date.today()).days

        if days_remaining <= 7:
            msg = format_token_warning("META_ACCESS_TOKEN", days_remaining)
            send_slack(msg)
            logger.warning(f"Meta token expires in {days_remaining} days!")
    except Exception:
        logger.exception("Failed to check Meta token expiry")


def _run_single_pipeline(name: str, days: int | None) -> dict:
    """Run a single pipeline, returning result dict. Never raises."""
    module_path, default_days, dataset = PIPELINES[name]
    effective_days = days if days is not None else default_days

    started = datetime.now(timezone.utc)
    t0 = time.monotonic()

    try:
        # Dynamic import
        import importlib

        mod = importlib.import_module(module_path)
        run_fn = mod.run_pipeline

        kwargs = {
            "destination": "bigquery",
            "dataset_name": dataset,
            "days_back": effective_days,
        }

        load_info = run_fn(**kwargs)
        duration = time.monotonic() - t0
        rows = _extract_row_count(load_info)
        finished = datetime.now(timezone.utc)

        log_run(
            pipeline_name=name,
            status="success",
            started_at=started,
            finished_at=finished,
            rows_loaded=rows,
            duration_seconds=duration,
        )

        return {"status": "success", "rows": rows, "duration": duration, "error": None}

    except Exception as exc:
        duration = time.monotonic() - t0
        finished = datetime.now(timezone.utc)
        error_msg = str(exc)[:500]

        log_run(
            pipeline_name=name,
            status="error",
            started_at=started,
            finished_at=finished,
            duration_seconds=duration,
            error_message=error_msg,
        )

        logger.exception(f"Pipeline {name} failed")
        return {"status": "error", "rows": 0, "duration": duration, "error": error_msg}


def _run_dbt(project_dir: str | None = None) -> tuple[bool, str]:
    """Run dbt run + dbt test + dbt source freshness. Returns (ok, output)."""
    if project_dir is None:
        # Default: dbt_project/ relative to repo root
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        project_dir = os.path.join(repo_root, "dbt_project")

    outputs = []

    for cmd_label, cmd in [
        ("dbt deps", ["dbt", "deps"]),
        ("dbt run", ["dbt", "run"]),
        ("dbt test", ["dbt", "test"]),
        ("dbt source freshness", ["dbt", "source", "freshness"]),
    ]:
        try:
            result = subprocess.run(
                cmd,
                cwd=project_dir,
                capture_output=True,
                text=True,
                timeout=600,
            )
            outputs.append(f"--- {cmd_label} ---\n{result.stdout}")
            if result.returncode != 0:
                # source freshness warnings are non-fatal
                if cmd_label == "dbt source freshness":
                    outputs.append(f"(freshness check returned {result.returncode})")
                    logger.warning(f"dbt source freshness returned {result.returncode}")
                else:
                    outputs.append(f"STDERR:\n{result.stderr}")
                    return False, "\n".join(outputs)
        except subprocess.TimeoutExpired:
            outputs.append(f"{cmd_label} timed out after 10 minutes")
            return False, "\n".join(outputs)
        except FileNotFoundError:
            outputs.append(f"dbt not found — is it installed? (pip install dbt-bigquery)")
            return False, "\n".join(outputs)

    return True, "\n".join(outputs)


def run_all(
    days: int | None = None,
    skip: list[str] | None = None,
    run_dbt: bool = True,
    send_notifications: bool = True,
) -> dict:
    """Run all pipelines + dbt. Returns {pipeline: {status, rows, duration, error}}.

    Args:
        days: Override days-back for all pipelines (None = use per-pipeline defaults)
        skip: List of pipeline names to skip
        run_dbt: Whether to run dbt after pipelines
        send_notifications: Whether to send Slack notifications
    """
    skip = skip or []
    t0 = time.monotonic()

    # Pre-flight: check Meta token expiry
    if send_notifications:
        _check_meta_token_expiry()

    # Run each pipeline with error isolation
    results = {}
    for name in PIPELINES:
        if name in skip:
            results[name] = {"status": "skipped", "rows": 0, "duration": 0, "error": None}
            logger.info(f"Skipping {name}")
            continue

        logger.info(f"=== Running {name} ===")
        results[name] = _run_single_pipeline(name, days)

    # Run dbt if any pipeline succeeded
    dbt_ok = True
    any_success = any(r["status"] == "success" for r in results.values())

    if run_dbt and any_success:
        logger.info("=== Running dbt ===")
        dbt_started = datetime.now(timezone.utc)
        dbt_t0 = time.monotonic()
        dbt_ok, dbt_output = _run_dbt()
        dbt_duration = time.monotonic() - dbt_t0
        dbt_finished = datetime.now(timezone.utc)

        log_run(
            pipeline_name="dbt",
            status="success" if dbt_ok else "error",
            started_at=dbt_started,
            finished_at=dbt_finished,
            duration_seconds=dbt_duration,
            error_message="" if dbt_ok else dbt_output[-500:],
        )

        if not dbt_ok:
            logger.error(f"dbt failed:\n{dbt_output}")
    elif run_dbt:
        logger.warning("Skipping dbt — no pipeline succeeded")
        dbt_ok = False

    # Send Slack summary
    total_duration = time.monotonic() - t0
    if send_notifications:
        msg = format_pipeline_summary(results, dbt_ok, total_duration)
        send_slack(msg)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Run all pipelines + dbt with error isolation"
    )
    parser.add_argument(
        "--days", type=int, default=None,
        help="Override days-back for all pipelines (default: per-pipeline)",
    )
    parser.add_argument(
        "--skip", type=str, default="",
        help="Comma-separated pipeline names to skip (e.g. shopify,meta-ads)",
    )
    parser.add_argument("--no-dbt", action="store_true", help="Skip dbt run")
    parser.add_argument("--no-slack", action="store_true", help="Skip Slack notifications")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    skip = [s.strip() for s in args.skip.split(",") if s.strip()]

    results = run_all(
        days=args.days,
        skip=skip,
        run_dbt=not args.no_dbt,
        send_notifications=not args.no_slack,
    )

    # Exit with error code if any pipeline failed
    any_failed = any(r["status"] == "error" for r in results.values())
    sys.exit(1 if any_failed else 0)


if __name__ == "__main__":
    main()

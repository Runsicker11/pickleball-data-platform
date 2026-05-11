"""dlt pipeline entry point for QuickBooks Online."""

import logging
import os
import re

import dlt

from ..config import (
    BIGQUERY_LOCATION,
    GCP_PROJECT_ID,
    PIPELINE_ROLLING_DAYS,
    QUICKBOOKS_CLIENT_ID,
    QUICKBOOKS_CLIENT_SECRET,
    QUICKBOOKS_REALM_ID,
    QUICKBOOKS_REFRESH_TOKEN,
)
from .source import quickbooks_source

logger = logging.getLogger(__name__)

PIPELINE_ENV_SECRET_NAME = "pipeline-env"
QB_REFRESH_TOKEN_KEY = "QUICKBOOKS_REFRESH_TOKEN"


def _persist_refresh_token_to_secret_manager(new_token: str) -> None:
    """Write a rotated QuickBooks refresh token back to the ``pipeline-env`` secret.

    QBO rotates the refresh token on every successful exchange. Without
    persistence, the next pipeline run uses the original token, which Intuit
    eventually invalidates — silently breaking nightly runs.

    This is best-effort: failures are logged but don't fail the pipeline,
    because the token in memory is still good for the current run.
    """
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{GCP_PROJECT_ID}/secrets/{PIPELINE_ENV_SECRET_NAME}"

    current = client.access_secret_version(
        request={"name": f"{secret_path}/versions/latest"}
    ).payload.data.decode("utf-8")

    pattern = rf"^{QB_REFRESH_TOKEN_KEY}=.*$"
    if re.search(pattern, current, flags=re.MULTILINE):
        updated = re.sub(pattern, f"{QB_REFRESH_TOKEN_KEY}={new_token}", current, flags=re.MULTILINE)
    else:
        updated = current.rstrip("\n") + f"\n{QB_REFRESH_TOKEN_KEY}={new_token}\n"

    if updated == current:
        logger.info("QuickBooks refresh token already up-to-date in Secret Manager")
        return

    client.add_secret_version(
        request={"parent": secret_path, "payload": {"data": updated.encode("utf-8")}}
    )
    logger.info("Persisted rotated QuickBooks refresh token to Secret Manager")


def run_pipeline(
    destination: str = "bigquery",
    dataset_name: str = "raw_quickbooks",
    days_back: int | None = None,
) -> dlt.pipeline:
    """Run the QuickBooks dlt pipeline.

    Args:
        destination: "bigquery" or "duckdb"
        dataset_name: Target dataset name
        days_back: Override for rolling window (default: 90 days for accounting data)
    """
    if days_back is None:
        days_back = max(PIPELINE_ROLLING_DAYS, 90)  # accounting data needs wider window

    if destination == "bigquery":
        dest = dlt.destinations.bigquery(
            credentials={
                "project_id": GCP_PROJECT_ID,
                "location": BIGQUERY_LOCATION,
            }
        )
    else:
        dest = destination

    pipeline = dlt.pipeline(
        pipeline_name="quickbooks",
        destination=dest,
        dataset_name=dataset_name,
    )

    # Only persist rotated tokens when running inside a Cloud Run Job — local
    # runs would otherwise create unwanted secret versions on every refresh.
    persister = (
        _persist_refresh_token_to_secret_manager
        if os.environ.get("CLOUD_RUN_JOB")
        else None
    )

    source = quickbooks_source(
        client_id=QUICKBOOKS_CLIENT_ID,
        client_secret=QUICKBOOKS_CLIENT_SECRET,
        refresh_token=QUICKBOOKS_REFRESH_TOKEN,
        realm_id=QUICKBOOKS_REALM_ID,
        days_back=days_back,
        on_refresh_token_change=persister,
    )

    logger.info(
        f"Running QuickBooks pipeline: dest={destination}, dataset={dataset_name}, "
        f"days={days_back}"
    )

    load_info = pipeline.run(source)
    logger.info(f"Pipeline complete: {load_info}")
    return load_info

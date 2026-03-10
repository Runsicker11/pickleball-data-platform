"""Pipeline health logging — writes run metadata to BigQuery ops.pipeline_runs.

Usage:
    from pipelines.health import log_run
    log_run("shopify", "success", rows_loaded=150, duration_seconds=45.2)
"""

import logging
import os
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery

from .config import BIGQUERY_LOCATION, GCP_PROJECT_ID

logger = logging.getLogger(__name__)

OPS_DATASET = "ops"
RUNS_TABLE = "pipeline_runs"
FULL_TABLE_ID = f"{GCP_PROJECT_ID}.{OPS_DATASET}.{RUNS_TABLE}"

SCHEMA = [
    bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("pipeline_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("finished_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("rows_loaded", "INTEGER"),
    bigquery.SchemaField("duration_seconds", "FLOAT64"),
    bigquery.SchemaField("error_message", "STRING"),
    bigquery.SchemaField("git_sha", "STRING"),
]


def _get_git_sha() -> str:
    """Get current git SHA, or empty string if unavailable."""
    # GitHub Actions sets this automatically
    sha = os.getenv("GITHUB_SHA", "")
    if sha:
        return sha[:8]
    try:
        import subprocess

        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else ""
    except Exception:
        return ""


def _ensure_table(client: bigquery.Client) -> None:
    """Create ops.pipeline_runs table if it doesn't exist."""
    dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, OPS_DATASET)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = BIGQUERY_LOCATION
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Created dataset {OPS_DATASET}")

    table_ref = dataset_ref.table(RUNS_TABLE)
    try:
        client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=SCHEMA)
        client.create_table(table)
        logger.info(f"Created table {FULL_TABLE_ID}")


def log_run(
    pipeline_name: str,
    status: str,
    started_at: datetime,
    finished_at: datetime | None = None,
    rows_loaded: int = 0,
    duration_seconds: float = 0.0,
    error_message: str = "",
) -> None:
    """Log a pipeline run to BigQuery ops.pipeline_runs.

    Never raises — wraps all errors so it can't break the pipeline.
    """
    if finished_at is None:
        finished_at = datetime.now(timezone.utc)

    row = {
        "run_id": str(uuid.uuid4()),
        "pipeline_name": pipeline_name,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "status": status,
        "rows_loaded": rows_loaded,
        "duration_seconds": duration_seconds,
        "error_message": error_message,
        "git_sha": _get_git_sha(),
    }

    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        _ensure_table(client)
        errors = client.insert_rows_json(FULL_TABLE_ID, [row])
        if errors:
            logger.warning(f"BigQuery insert errors: {errors}")
        else:
            logger.info(f"Logged run: {pipeline_name} → {status}")
    except Exception:
        logger.exception(f"Failed to log run for {pipeline_name}")

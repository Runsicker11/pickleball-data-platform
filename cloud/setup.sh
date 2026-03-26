#!/usr/bin/env bash
#
# One-time setup for daily pipeline automation via Cloud Scheduler + Cloud Run Jobs.
# Each pipeline runs as its own independent job — failures are isolated.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated (gcloud auth login)
#   - Both repos cloned side-by-side:
#       .../pickleball-data-platform/
#       .../ai-marketing/
#   - .env files populated in both repos
#
# Usage:
#   cd pickleball-data-platform/cloud
#   bash setup.sh
#
set -euo pipefail

# ── Architecture ───────────────────────────────────────────────────────────
#
#  ALL data ingestion and transformation flows through this repo:
#
#    dlt pipelines (pipelines/)          → raw_*  BigQuery datasets
#    dbt models    (dbt_project/)        → stg_*, int_*, bi, marketing_data (vw_*)
#    ai-marketing  (../ai-marketing)     → reads marketing_data.vw_* (analysis only)
#
#  Daily schedule (UTC):
#    6:00 AM  — all 8 pipelines run in parallel (shopify, meta-ads, google-ads,
#               search-console, amazon-ads, amazon-seller, quickbooks, paypal)
#    8:30 AM  — dbt  (transforms raw_* into staging → intermediate → mart/compat views)
#    9:00 AM  — daily-analysis  (Claude-powered analysis reads marketing_data.vw_*)
#
#  The marketing_data.* BASE TABLEs from the old ai-marketing ingestion module
#  have been removed. All data in marketing_data is now managed by dbt views.
#
# ── Configuration ──────────────────────────────────────────────────────────
PROJECT_ID="practical-gecko-373320"
REGION="us-west1"
SA_NAME="pipeline-runner"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
AR_REPO="data-platform"

PIPELINE_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/data-pipeline"
ANALYSIS_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/daily-analysis"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
AI_MARKETING_ROOT="$(cd "${REPO_ROOT}/../ai-marketing" && pwd)"

echo "=== Daily Pipeline Automation Setup ==="
echo "Project:  ${PROJECT_ID}"
echo "Region:   ${REGION}"
echo "Pipeline: ${REPO_ROOT}"
echo "Analysis: ${AI_MARKETING_ROOT}"
echo ""

# ── 1. Enable required APIs ───────────────────────────────────────────────
echo "1/8  Enabling APIs..."
gcloud services enable \
    run.googleapis.com \
    cloudscheduler.googleapis.com \
    secretmanager.googleapis.com \
    artifactregistry.googleapis.com \
    cloudbuild.googleapis.com \
    --project="${PROJECT_ID}" --quiet

# ── 2. Create service account ─────────────────────────────────────────────
echo "2/8  Creating service account..."
gcloud iam service-accounts create "${SA_NAME}" \
    --display-name="Pipeline Runner" \
    --project="${PROJECT_ID}" 2>/dev/null \
    || echo "     (already exists)"

# ── 3. Grant IAM roles ────────────────────────────────────────────────────
echo "3/8  Granting IAM roles..."
for ROLE in \
    roles/bigquery.admin \
    roles/run.developer \
    roles/secretmanager.secretAccessor; do
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="${ROLE}" --quiet > /dev/null
done
echo "     bigquery.admin, run.developer, secretmanager.secretAccessor"

# ── 4. Create Artifact Registry repo ──────────────────────────────────────
echo "4/8  Creating Artifact Registry repo..."
gcloud artifacts repositories create "${AR_REPO}" \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${PROJECT_ID}" 2>/dev/null \
    || echo "     (already exists)"

# ── 5. Store .env files as Secret Manager secrets ─────────────────────────
echo "5/8  Storing credentials in Secret Manager..."

for SECRET_NAME in pipeline-env analysis-env; do
    if [ "${SECRET_NAME}" = "pipeline-env" ]; then
        ENV_FILE="${REPO_ROOT}/.env"
    else
        ENV_FILE="${AI_MARKETING_ROOT}/.env"
    fi

    if [ ! -f "${ENV_FILE}" ]; then
        echo "     ERROR: ${ENV_FILE} not found. Create it first."
        exit 1
    fi

    if gcloud secrets describe "${SECRET_NAME}" --project="${PROJECT_ID}" > /dev/null 2>&1; then
        gcloud secrets versions add "${SECRET_NAME}" \
            --data-file="${ENV_FILE}" \
            --project="${PROJECT_ID}" --quiet
        echo "     ${SECRET_NAME}: updated"
    else
        gcloud secrets create "${SECRET_NAME}" \
            --data-file="${ENV_FILE}" \
            --project="${PROJECT_ID}" --quiet
        echo "     ${SECRET_NAME}: created"
    fi
done

# ── 6. Build and push Docker images ───────────────────────────────────────
echo "6/8  Building Docker images (this takes a few minutes)..."

echo "     Building data-pipeline..."
gcloud builds submit "${REPO_ROOT}" \
    --tag="${PIPELINE_IMAGE}" \
    --project="${PROJECT_ID}" --quiet

echo "     Building daily-analysis..."
gcloud builds submit "${AI_MARKETING_ROOT}" \
    --tag="${ANALYSIS_IMAGE}" \
    --project="${PROJECT_ID}" --quiet

# ── 7. Create/update individual pipeline Cloud Run Jobs ───────────────────
echo "7/8  Creating pipeline Cloud Run Jobs (one per pipeline)..."

# Helper: create or update a Cloud Run Job
_upsert_job() {
    local JOB_NAME="$1"
    local TIMEOUT="$2"
    local CMD="$3"

    local COMMON_ARGS=(
        --image="${PIPELINE_IMAGE}"
        --region="${REGION}"
        --project="${PROJECT_ID}"
        --service-account="${SA_EMAIL}"
        --set-secrets="/secrets/.env=pipeline-env:latest"
        --task-timeout="${TIMEOUT}"
        --max-retries=0
        --memory=2Gi
        --cpu=2
        --command="sh"
        --args="-c,cp /secrets/.env /app/.env 2>/dev/null; ${CMD}"
        --quiet
    )

    gcloud run jobs create "${JOB_NAME}" "${COMMON_ARGS[@]}" 2>/dev/null \
        || gcloud run jobs update "${JOB_NAME}" "${COMMON_ARGS[@]}"

    echo "     ${JOB_NAME} (timeout: ${TIMEOUT}s)"
}

# Individual pipeline jobs
_upsert_job "pipeline-shopify"          900   "python -m pipelines.run shopify --days 3"
_upsert_job "pipeline-meta-ads"         1800  "python -m pipelines.run meta-ads --days 3"
_upsert_job "pipeline-google-ads"       1800  "python -m pipelines.run google-ads --days 7"
_upsert_job "pipeline-search-console"   1800  "python -m pipelines.run search-console --days 7"
_upsert_job "pipeline-amazon-ads"       7200  "python -m pipelines.run amazon-ads --days 7"
_upsert_job "pipeline-amazon-seller"    3600  "python -m pipelines.run amazon-seller --days 30"
_upsert_job "pipeline-quickbooks"       1800  "python -m pipelines.run quickbooks --days 90"
_upsert_job "pipeline-paypal"           1800  "python -m pipelines.run paypal --days 365"
_upsert_job "pipeline-klaviyo"          1800  "python -m pipelines.run klaviyo --days 7"

# dbt job (runs after all pipelines complete)
# dbt test and source freshness are non-fatal: failures are logged but don't fail the job
# (consistent with how run_all.py handled dbt — run failure = fatal, test failure = warning)
_upsert_job "pipeline-dbt"              1800  "cd /app/dbt_project && dbt deps && dbt run && { dbt test || echo 'dbt test had failures — check logs'; } && { dbt source freshness || echo 'source freshness had warnings'; }"

# Keep the daily-analysis job (ai-marketing repo)
gcloud run jobs create daily-analysis \
    --image="${ANALYSIS_IMAGE}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --service-account="${SA_EMAIL}" \
    --set-secrets="/secrets/.env=analysis-env:latest" \
    --task-timeout=900 \
    --max-retries=0 \
    --memory=1Gi \
    --cpu=1 \
    --quiet 2>/dev/null \
    || gcloud run jobs update daily-analysis \
        --image="${ANALYSIS_IMAGE}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --service-account="${SA_EMAIL}" \
        --set-secrets="/secrets/.env=analysis-env:latest" \
        --task-timeout=900 \
        --max-retries=0 \
        --memory=1Gi \
        --cpu=1 \
        --quiet

# ── 8. Create Cloud Scheduler triggers ────────────────────────────────────
echo "8/8  Creating Cloud Scheduler triggers..."

# Helper: create or skip (can't update scheduler URIs easily)
_upsert_scheduler() {
    local TRIGGER_NAME="$1"
    local SCHEDULE="$2"
    local JOB_NAME="$3"

    local JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

    gcloud scheduler jobs create http "${TRIGGER_NAME}" \
        --schedule="${SCHEDULE}" \
        --time-zone="UTC" \
        --uri="${JOB_URI}" \
        --http-method=POST \
        --oauth-service-account-email="${SA_EMAIL}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --quiet 2>/dev/null \
        && echo "     ${TRIGGER_NAME}: created" \
        || echo "     ${TRIGGER_NAME}: already exists (delete + recreate to update)"
}

# All pipeline jobs trigger simultaneously at 6:00 AM UTC
# dbt triggers at 8:30 AM UTC — 2.5 hours later, after even the slowest pipeline (amazon-ads) has finished
_upsert_scheduler "trigger-shopify"          "0 6 * * *"    "pipeline-shopify"
_upsert_scheduler "trigger-meta-ads"         "0 6 * * *"    "pipeline-meta-ads"
_upsert_scheduler "trigger-google-ads"       "0 6 * * *"    "pipeline-google-ads"
_upsert_scheduler "trigger-search-console"   "0 6 * * *"    "pipeline-search-console"
_upsert_scheduler "trigger-amazon-ads"       "0 6 * * *"    "pipeline-amazon-ads"
_upsert_scheduler "trigger-amazon-seller"    "0 6 * * *"    "pipeline-amazon-seller"
_upsert_scheduler "trigger-quickbooks"       "0 6 * * *"    "pipeline-quickbooks"
_upsert_scheduler "trigger-paypal"           "0 6 * * *"    "pipeline-paypal"
_upsert_scheduler "trigger-klaviyo"          "0 6 * * *"    "pipeline-klaviyo"
_upsert_scheduler "trigger-dbt"              "30 8 * * *"   "pipeline-dbt"

# Analysis job: 9:00 AM UTC (after dbt finishes)
ANALYSIS_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/daily-analysis:run"
gcloud scheduler jobs create http daily-analysis-trigger \
    --schedule="0 9 * * *" \
    --time-zone="UTC" \
    --uri="${ANALYSIS_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --quiet 2>/dev/null \
    && echo "     daily-analysis-trigger: created" \
    || echo "     daily-analysis-trigger: already exists"

# ── 9. Remove old monolithic job (if it exists) ────────────────────────────
echo ""
echo "Cleaning up old monolithic job..."
gcloud run jobs delete data-pipeline \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --quiet 2>/dev/null \
    && echo "     data-pipeline job: deleted" \
    || echo "     data-pipeline job: not found (already removed)"

gcloud scheduler jobs delete daily-pipeline-trigger \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --quiet 2>/dev/null \
    && echo "     daily-pipeline-trigger: deleted" \
    || echo "     daily-pipeline-trigger: not found (already removed)"

# ── Done ──────────────────────────────────────────────────────────────────
echo ""
echo "=== Setup complete! ==="
echo ""
echo "Schedule (UTC):"
echo "  6:00 AM  →  shopify, meta-ads, google-ads, search-console,"
echo "              amazon-ads, amazon-seller, quickbooks, paypal  (parallel)"
echo "  8:30 AM  →  dbt  (after all pipelines have finished)"
echo "  9:00 AM  →  daily-analysis"
echo ""
echo "Test a pipeline:"
echo "  gcloud run jobs execute pipeline-google-ads --region ${REGION}"
echo "  gcloud run jobs execute pipeline-amazon-ads --region ${REGION}"
echo "  gcloud run jobs execute pipeline-dbt --region ${REGION}"
echo ""
echo "View logs:"
echo "  https://console.cloud.google.com/run/jobs?project=${PROJECT_ID}"
echo ""
echo "Rebuild image after code changes:"
echo "  gcloud builds submit ${REPO_ROOT} --tag=${PIPELINE_IMAGE}"
echo ""
echo "Per-pipeline timeouts:"
echo "  shopify:          900s  (15 min)"
echo "  meta-ads:        1800s  (30 min)"
echo "  google-ads:      1800s  (30 min)"
echo "  search-console:  1800s  (30 min)"
echo "  amazon-ads:      7200s  (2 hours — async reports)"
echo "  amazon-seller:   3600s  (1 hour)"
echo "  quickbooks:      1800s  (30 min)"
echo "  paypal:          1800s  (30 min)"
echo "  dbt:             1800s  (30 min)"

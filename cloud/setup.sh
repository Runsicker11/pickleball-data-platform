#!/usr/bin/env bash
#
# One-time setup for daily pipeline automation via Cloud Scheduler + Cloud Run Jobs.
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

    # Create or update
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

# ── 7. Create Cloud Run Jobs ─────────────────────────────────────────────
echo "7/8  Creating Cloud Run Jobs..."

gcloud run jobs create data-pipeline \
    --image="${PIPELINE_IMAGE}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --service-account="${SA_EMAIL}" \
    --set-secrets="/secrets/.env=pipeline-env:latest" \
    --task-timeout=3600 \
    --max-retries=0 \
    --memory=2Gi \
    --cpu=2 \
    --quiet 2>/dev/null \
    || gcloud run jobs update data-pipeline \
        --image="${PIPELINE_IMAGE}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --service-account="${SA_EMAIL}" \
        --set-secrets="/secrets/.env=pipeline-env:latest" \
        --task-timeout=3600 \
        --max-retries=0 \
        --memory=2Gi \
        --cpu=2 \
        --quiet

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

# Pipeline job: 6:00 AM UTC (12:00 AM CST)
PIPELINE_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/data-pipeline:run"

gcloud scheduler jobs create http daily-pipeline-trigger \
    --schedule="0 6 * * *" \
    --time-zone="UTC" \
    --uri="${PIPELINE_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --quiet 2>/dev/null \
    || echo "     daily-pipeline-trigger: already exists (delete + recreate to update)"

# Analysis job: 6:45 AM UTC (45 min after pipeline starts)
ANALYSIS_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/daily-analysis:run"

gcloud scheduler jobs create http daily-analysis-trigger \
    --schedule="45 6 * * *" \
    --time-zone="UTC" \
    --uri="${ANALYSIS_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --quiet 2>/dev/null \
    || echo "     daily-analysis-trigger: already exists (delete + recreate to update)"

# ── Done ──────────────────────────────────────────────────────────────────
echo ""
echo "=== Setup complete! ==="
echo ""
echo "Schedule:"
echo "  6:00 AM UTC  →  data-pipeline   (pipelines + dbt, ~15 min)"
echo "  6:45 AM UTC  →  daily-analysis  (alerts + weekly on Sundays)"
echo ""
echo "Test commands:"
echo "  gcloud run jobs execute data-pipeline --region ${REGION}"
echo "  gcloud run jobs execute daily-analysis --region ${REGION}"
echo ""
echo "View logs:"
echo "  https://console.cloud.google.com/run/jobs?project=${PROJECT_ID}"
echo ""
echo "Update after code changes:"
echo "  gcloud builds submit ${REPO_ROOT} --tag=${PIPELINE_IMAGE}"
echo "  gcloud run jobs update data-pipeline --image=${PIPELINE_IMAGE} --region=${REGION}"

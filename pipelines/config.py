"""Shared configuration for all pipelines."""

import os

from dotenv import load_dotenv

load_dotenv(override=True)

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "practical-gecko-373320")
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "us-west1")

# Amazon Ads
AMAZON_CLIENT_ID = os.getenv("AMAZON_CLIENT_ID", "")
AMAZON_CLIENT_SECRET = os.getenv("AMAZON_CLIENT_SECRET", "")
AMAZON_REFRESH_TOKEN = os.getenv("AMAZON_REFRESH_TOKEN", "")
AMAZON_PROFILE_IDS = [
    p.strip() for p in os.getenv("AMAZON_PROFILE_IDS", "").split(",") if p.strip()
]

# Pipeline defaults
PIPELINE_ROLLING_DAYS = int(os.getenv("PIPELINE_ROLLING_DAYS", "7"))
PIPELINE_POLL_INTERVAL = int(os.getenv("PIPELINE_POLL_INTERVAL", "120"))
PIPELINE_POLL_TIMEOUT_MINUTES = int(os.getenv("PIPELINE_POLL_TIMEOUT_MINUTES", "45"))

"""Builds a write-scoped ShopifyClient using credentials from the writer-env secret.

The pipeline-env Shopify token is read-only by design (data ingestion). For
content writes (image alts, pages), there's a separate write-scoped custom app
whose credentials live in the writer-env Secret Manager secret.

Both apps target the same shop. The write app uses Client Credentials Grant —
the existing ShopifyClient handles that automatically when access_token is empty.
"""

import logging
import subprocess

from pipelines import config
from pipelines.shopify.client import ShopifyClient

logger = logging.getLogger(__name__)

WRITER_SECRET_NAME = "writer-env"
GCP_PROJECT = "practical-gecko-373320"


def _fetch_writer_env() -> dict[str, str]:
    """Pull writer-env from Secret Manager and parse to dict."""
    result = subprocess.run(
        [
            "gcloud", "secrets", "versions", "access", "latest",
            f"--secret={WRITER_SECRET_NAME}",
            f"--project={GCP_PROJECT}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to fetch {WRITER_SECRET_NAME} from Secret Manager: {result.stderr}"
        )
    env: dict[str, str] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip()
    return env


def get_writer_client() -> ShopifyClient:
    """Construct a write-scoped ShopifyClient.

    Uses writer-env Secret Manager creds. Caches the access token for the
    lifetime of the process (ShopifyClient holds it after auth).
    """
    env = _fetch_writer_env()

    shop_domain = env.get("SHOPIFY_SHOP_DOMAIN") or config.SHOPIFY_SHOP_DOMAIN
    client_id = env.get("SHOPIFY_WRITER_CLIENT_ID", "")
    client_secret = env.get("SHOPIFY_WRITER_CLIENT_SECRET", "")
    api_version = env.get("SHOPIFY_API_VERSION") or config.SHOPIFY_API_VERSION

    if not (client_id and client_secret):
        raise RuntimeError(
            "writer-env is missing SHOPIFY_WRITER_CLIENT_ID or "
            "SHOPIFY_WRITER_CLIENT_SECRET — check the Secret Manager entry."
        )

    logger.info("Authenticating to Shopify with writer credentials (Client Credentials Grant)")
    return ShopifyClient(
        shop_domain=shop_domain,
        client_id=client_id,
        client_secret=client_secret,
        access_token="",  # forces Client Credentials Grant
        api_version=api_version,
    )

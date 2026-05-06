"""Google Ads Customer Match export: BigQuery stg_shopify__customers → Google Ads API.

Reads email, first_name, last_name and SHA256-hashes them per Google's spec,
then upserts (ADD) into the "Shopify Customers — All Time" CRM user list.
Phone is not currently collected in the Shopify pipeline; add it to
pipelines/shopify/source.py fields + stg_shopify__customers to improve match rates.
"""

import hashlib
import logging

from google.ads.googleads.client import GoogleAdsClient

from ..config import (
    GCP_PROJECT_ID,
    GOOGLE_ADS_CLIENT_ID,
    GOOGLE_ADS_CLIENT_SECRET,
    GOOGLE_ADS_CUSTOMER_ID,
    GOOGLE_ADS_DEVELOPER_TOKEN,
    GOOGLE_ADS_LOGIN_CUSTOMER_ID,
    GOOGLE_ADS_REFRESH_TOKEN,
)

logger = logging.getLogger(__name__)

_USER_LIST_NAME = "Shopify Customers — All Time"
_BQ_TABLE = f"{GCP_PROJECT_ID}.stg_shopify.stg_shopify__customers"
_BATCH_SIZE = 10_000


def _sha256(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()


def _build_client() -> GoogleAdsClient:
    config = {
        "developer_token": GOOGLE_ADS_DEVELOPER_TOKEN,
        "client_id": GOOGLE_ADS_CLIENT_ID,
        "client_secret": GOOGLE_ADS_CLIENT_SECRET,
        "refresh_token": GOOGLE_ADS_REFRESH_TOKEN,
        "use_proto_plus": True,
    }
    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        config["login_customer_id"] = GOOGLE_ADS_LOGIN_CUSTOMER_ID
    return GoogleAdsClient.load_from_dict(config)


def _get_or_create_user_list(client: GoogleAdsClient, customer_id: str) -> str:
    """Return the resource name of the target user list, creating it if it doesn't exist."""
    ga_service = client.get_service("GoogleAdsService")
    safe_name = _USER_LIST_NAME.replace("'", "\\'")
    query = f"""
        SELECT user_list.resource_name
        FROM user_list
        WHERE user_list.name = '{safe_name}'
        LIMIT 1
    """
    rows = list(ga_service.search(customer_id=customer_id, query=query))
    if rows:
        rn = rows[0].user_list.resource_name
        logger.info("Using existing user list: %s", rn)
        return rn

    user_list_service = client.get_service("UserListService")
    op = client.get_type("UserListOperation")
    ul = op.create
    ul.name = _USER_LIST_NAME
    ul.description = "All Shopify customers — refreshed monthly via Customer Match export"
    ul.membership_status = client.enums.UserListMembershipStatusEnum.OPEN
    ul.membership_life_span = 10000  # max, effectively no expiry
    ul.crm_based_user_list.upload_key_type = (
        client.enums.CustomerMatchUploadKeyTypeEnum.CONTACT_INFO
    )
    resp = user_list_service.mutate_user_lists(customer_id=customer_id, operations=[op])
    rn = resp.results[0].resource_name
    logger.info("Created user list: %s", rn)
    return rn


def _query_bq() -> list[dict]:
    from google.cloud import bigquery

    bq = bigquery.Client(project=GCP_PROJECT_ID)
    sql = f"""
        SELECT email, first_name, last_name
        FROM `{_BQ_TABLE}`
        WHERE email IS NOT NULL AND TRIM(email) != ''
    """
    rows = list(bq.query(sql).result())
    logger.info("Loaded %d customers from BigQuery", len(rows))
    return [
        {"email": r.email, "first_name": r.first_name, "last_name": r.last_name}
        for r in rows
    ]


def _make_add_operation(client: GoogleAdsClient, row: dict):
    op = client.get_type("OfflineUserDataJobOperation")
    user_data = op.create

    email_id = client.get_type("UserIdentifier")
    email_id.hashed_email = _sha256(row["email"])
    user_data.user_identifiers.append(email_id)

    # Hashed name improves match rate (both fields required together)
    first_name = (row.get("first_name") or "").strip()
    last_name = (row.get("last_name") or "").strip()
    if first_name and last_name:
        name_id = client.get_type("UserIdentifier")
        name_id.address_identifier.hashed_first_name = _sha256(first_name)
        name_id.address_identifier.hashed_last_name = _sha256(last_name)
        user_data.user_identifiers.append(name_id)

    return op


def run_pipeline() -> int:
    """Export Shopify customers to Google Ads Customer Match. Returns count uploaded."""
    client = _build_client()
    customer_id = GOOGLE_ADS_CUSTOMER_ID.replace("-", "")

    user_list_rn = _get_or_create_user_list(client, customer_id)
    customers = _query_bq()

    if not customers:
        logger.warning("No customers with email found in BigQuery — skipping upload")
        return 0

    job_service = client.get_service("OfflineUserDataJobService")
    job = client.get_type("OfflineUserDataJob")
    job.type_ = client.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
    job.customer_match_user_list_metadata.user_list = user_list_rn
    # Consent signals required for EEA; US customers are opted-in via Shopify checkout
    job.customer_match_user_list_metadata.consent.ad_user_data = (
        client.enums.ConsentStatusEnum.GRANTED
    )
    job.customer_match_user_list_metadata.consent.ad_personalization = (
        client.enums.ConsentStatusEnum.GRANTED
    )

    create_resp = job_service.create_offline_user_data_job(
        customer_id=customer_id, job=job
    )
    job_rn = create_resp.resource_name
    logger.info("Created offline user data job: %s", job_rn)

    operations = [_make_add_operation(client, row) for row in customers]
    total_batches = -(-len(operations) // _BATCH_SIZE)  # ceiling division
    for i in range(0, len(operations), _BATCH_SIZE):
        batch = operations[i : i + _BATCH_SIZE]
        job_service.add_offline_user_data_job_operations(
            resource_name=job_rn, operations=batch
        )
        logger.info(
            "Uploaded batch %d/%d (%d users)",
            i // _BATCH_SIZE + 1,
            total_batches,
            len(batch),
        )

    job_service.run_offline_user_data_job(resource_name=job_rn)
    logger.info(
        "Customer Match job submitted for processing: %s — %d users total",
        job_rn,
        len(customers),
    )
    return len(customers)

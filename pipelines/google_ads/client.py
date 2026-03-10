"""Google Ads API client wrapper using the official google-ads library."""

import logging

from google.ads.googleads.client import GoogleAdsClient

logger = logging.getLogger(__name__)


class GoogleAdsApiClient:
    """Wraps GoogleAdsClient for GAQL queries."""

    def __init__(
        self,
        customer_id: str,
        developer_token: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        login_customer_id: str = "",
    ):
        self.customer_id = customer_id

        config = {
            "developer_token": developer_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "use_proto_plus": True,
        }
        if login_customer_id:
            config["login_customer_id"] = login_customer_id

        self._client = GoogleAdsClient.load_from_dict(config)
        self._ga_service = self._client.get_service("GoogleAdsService")

    def query(self, gaql: str) -> list:
        """Execute a GAQL query and return all result rows."""
        rows = []
        response = self._ga_service.search_stream(
            customer_id=self.customer_id, query=gaql
        )
        for batch in response:
            rows.extend(batch.results)
        return rows

    def validate_access(self):
        """Verify credentials by querying the customer resource."""
        query = """
            SELECT
                customer.descriptive_name,
                customer.currency_code,
                customer.id
            FROM customer
            LIMIT 1
        """
        rows = self.query(query)
        if not rows:
            raise RuntimeError("Google Ads credential validation failed — no customer data returned")
        row = rows[0]
        logger.info(
            f"Google Ads verified: {row.customer.descriptive_name} "
            f"(ID: {row.customer.id}, Currency: {row.customer.currency_code})"
        )

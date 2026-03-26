"""Google Merchant Center (Content API v2.1) client using service account auth."""

import json
import logging

from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/content"]


class MerchantCenterClient:
    """Wraps the Content API v2.1 with service account authentication."""

    def __init__(self, merchant_id: str, sa_key_json: str = ""):
        """
        Args:
            merchant_id: Merchant Center account ID (numeric string).
            sa_key_json: Service account key as a JSON string. Falls back to
                         Application Default Credentials if empty.
        """
        self.merchant_id = merchant_id

        if sa_key_json:
            from google.oauth2 import service_account

            key_data = json.loads(sa_key_json)
            creds = service_account.Credentials.from_service_account_info(
                key_data, scopes=_SCOPES
            )
        else:
            import google.auth

            creds, _ = google.auth.default(scopes=_SCOPES)

        self._service = build("content", "v2.1", credentials=creds)

    def validate_access(self):
        """Verify access by fetching the account info."""
        result = self._service.accounts().get(
            merchantId=self.merchant_id, accountId=self.merchant_id
        ).execute()
        logger.info(f"Merchant Center access verified: {result.get('name', self.merchant_id)}")

    def list_products(self) -> list[dict]:
        """Return all products via paginated list."""
        products = []
        request = self._service.products().list(merchantId=self.merchant_id, maxResults=250)
        while request is not None:
            response = request.execute()
            products.extend(response.get("resources", []))
            request = self._service.products().list_next(request, response)
        logger.info(f"Fetched {len(products)} products")
        return products

    def list_product_statuses(self) -> list[dict]:
        """Return all product statuses via paginated list."""
        statuses = []
        request = self._service.productstatuses().list(
            merchantId=self.merchant_id, maxResults=250
        )
        while request is not None:
            response = request.execute()
            statuses.extend(response.get("resources", []))
            request = self._service.productstatuses().list_next(request, response)
        logger.info(f"Fetched {len(statuses)} product statuses")
        return statuses

    def get_shopping_ads_program(self) -> dict:
        """Return the Shopping Ads program status. Requires Administrator access."""
        return self._service.shoppingadsprogram().get(merchantId=self.merchant_id).execute()

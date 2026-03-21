"""PayPal dlt source — transaction history."""

import logging
from datetime import datetime, timedelta, timezone

import dlt

from .client import PayPalClient

logger = logging.getLogger(__name__)


def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


_STATUS_MAP = {
    "S": "Success",
    "P": "Pending",
    "V": "Reversed",
    "D": "Denied",
    "F": "Failed",
}


@dlt.source(name="paypal")
def paypal_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    days_back: int = 365,
):
    """dlt source yielding PayPal transaction history."""
    client = PayPalClient(client_id=client_id, client_secret=client_secret)

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days_back)

    yield _transactions_resource(client, start_dt, end_dt)


def _transactions_resource(client: PayPalClient, start_dt: datetime, end_dt: datetime):
    @dlt.resource(
        name="transactions",
        write_disposition="merge",
        primary_key="transaction_id",
        merge_key="transaction_id",
        columns={
            "payer_email": {"data_type": "text", "nullable": True},
            "payer_account_id": {"data_type": "text", "nullable": True},
            "payer_given_name": {"data_type": "text", "nullable": True},
            "payer_surname": {"data_type": "text", "nullable": True},
            "payer_full_name": {"data_type": "text", "nullable": True},
            "payer_alternate_name": {"data_type": "text", "nullable": True},
            "first_item_name": {"data_type": "text", "nullable": True},
        },
    )
    def transactions():
        now_str = _now_utc_str()

        for raw in client.get_transactions(start_dt, end_dt):
            txn_info = raw.get("transaction_info") or {}
            payer_info = raw.get("payer_info") or {}
            payer_name = payer_info.get("payer_name") or {}
            cart_info = raw.get("cart_info") or {}
            items = cart_info.get("item_details") or []

            given_name = payer_name.get("given_name", "")
            surname = payer_name.get("surname", "")
            alternate_name = payer_name.get("alternate_name", "")
            full_name = " ".join(filter(None, [given_name, surname])).strip() or None

            first_item = items[0] if items else {}

            yield {
                "transaction_id": txn_info.get("transaction_id"),
                "transaction_date": txn_info.get("transaction_initiation_date"),
                "transaction_updated_date": txn_info.get("transaction_updated_date"),
                "transaction_amount": float(
                    (txn_info.get("transaction_amount") or {}).get("value", 0) or 0
                ),
                "fee_amount": float(
                    (txn_info.get("fee_amount") or {}).get("value", 0) or 0
                ),
                "currency_code": (txn_info.get("transaction_amount") or {}).get(
                    "currency_code"
                ),
                "transaction_status": _STATUS_MAP.get(
                    txn_info.get("transaction_status"), txn_info.get("transaction_status")
                ),
                "transaction_event_code": txn_info.get("transaction_event_code"),
                "transaction_subject": txn_info.get("transaction_subject"),
                "transaction_note": txn_info.get("transaction_note"),
                "paypal_reference_id": txn_info.get("paypal_reference_id"),
                "paypal_reference_id_type": txn_info.get("paypal_reference_id_type"),
                "invoice_id": txn_info.get("invoice_id"),
                "custom_field": txn_info.get("custom_field"),
                "payer_email": payer_info.get("email_address"),
                "payer_account_id": payer_info.get("account_id"),
                "payer_given_name": given_name or None,
                "payer_surname": surname or None,
                "payer_full_name": full_name,
                "payer_alternate_name": alternate_name or None,
                "first_item_name": first_item.get("item_name"),
                "item_count": len(items),
                "ingested_at": now_str,
            }

    return transactions

"""QuickBooks dlt source — invoices, payments, purchases, bills, deposits, customers, accounts, vendors, items."""

import logging
from datetime import date, datetime, timedelta, timezone

import dlt

from .client import QuickBooksClient

logger = logging.getLogger(__name__)


def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


@dlt.source(name="quickbooks")
def quickbooks_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    realm_id: str = dlt.secrets.value,
    days_back: int = 90,
):
    """dlt source yielding QuickBooks Online resources."""
    client = QuickBooksClient(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        realm_id=realm_id,
    )
    since_date = date.today() - timedelta(days=days_back)

    yield _invoices_resource(client, since_date)
    yield _invoice_line_items_resource(client, since_date)
    yield _payments_resource(client, since_date)
    yield _sales_receipts_resource(client, since_date)
    yield _deposits_resource(client, since_date)
    yield _purchases_resource(client, since_date)
    yield _purchase_line_items_resource(client, since_date)
    yield _bills_resource(client, since_date)
    yield _bill_payments_resource(client, since_date)
    yield _customers_resource(client)
    yield _vendors_resource(client)
    yield _accounts_resource(client)
    yield _items_resource(client)


def _invoices_resource(client: QuickBooksClient, since_date: date):
    @dlt.resource(
        name="invoices",
        write_disposition="merge",
        merge_key="invoice_id",
        primary_key="invoice_id",
    )
    def invoices():
        now_str = _now_utc_str()
        where = f"TxnDate >= '{since_date.isoformat()}'"
        raw = client.query("Invoice", where=where, order_by="TxnDate DESC")
        logger.info(f"Fetched {len(raw)} invoices since {since_date}")

        for inv in raw:
            customer_ref = inv.get("CustomerRef") or {}
            yield {
                "invoice_id": inv["Id"],
                "doc_number": inv.get("DocNumber"),
                "txn_date": inv.get("TxnDate"),
                "due_date": inv.get("DueDate"),
                "customer_id": customer_ref.get("value"),
                "customer_name": customer_ref.get("name"),
                "total_amount": float(inv.get("TotalAmt", 0)),
                "balance": float(inv.get("Balance", 0)),
                "currency": inv.get("CurrencyRef", {}).get("value", "USD"),
                "email_status": inv.get("EmailStatus"),
                "billing_email": inv.get("BillEmail", {}).get("Address"),
                "ship_date": inv.get("ShipDate"),
                "tracking_num": inv.get("TrackingNum"),
                "apply_tax_after_discount": inv.get("ApplyTaxAfterDiscount"),
                "txn_status": _invoice_status(inv),
                "created_at": inv.get("MetaData", {}).get("CreateTime"),
                "updated_at": inv.get("MetaData", {}).get("LastUpdatedTime"),
                "ingested_at": now_str,
            }

    return invoices


def _invoice_status(inv: dict) -> str:
    """Derive invoice status from balance."""
    balance = float(inv.get("Balance", 0))
    total = float(inv.get("TotalAmt", 0))
    if balance == 0 and total > 0:
        return "Paid"
    elif balance < total:
        return "Partial"
    elif balance == total:
        return "Open"
    return "Unknown"


def _invoice_line_items_resource(client: QuickBooksClient, since_date: date):
    @dlt.resource(
        name="invoice_line_items",
        write_disposition="merge",
        merge_key="line_item_id",
        primary_key="line_item_id",
    )
    def invoice_line_items():
        now_str = _now_utc_str()
        where = f"TxnDate >= '{since_date.isoformat()}'"
        raw = client.query("Invoice", where=where, order_by="TxnDate DESC")

        for inv in raw:
            invoice_id = inv["Id"]
            txn_date = inv.get("TxnDate")
            customer_ref = inv.get("CustomerRef") or {}

            for line in inv.get("Line", []):
                detail = line.get("SalesItemLineDetail") or {}
                item_ref = detail.get("ItemRef") or {}
                # Skip subtotal lines
                if line.get("DetailType") == "SubTotalLineDetail":
                    continue

                yield {
                    "line_item_id": f"{invoice_id}_{line.get('Id', '0')}",
                    "invoice_id": invoice_id,
                    "txn_date": txn_date,
                    "customer_id": customer_ref.get("value"),
                    "line_num": line.get("LineNum"),
                    "description": line.get("Description"),
                    "item_id": item_ref.get("value"),
                    "item_name": item_ref.get("name"),
                    "quantity": float(detail.get("Qty", 0)),
                    "unit_price": float(detail.get("UnitPrice", 0)),
                    "amount": float(line.get("Amount", 0)),
                    "discount_rate": float(detail.get("DiscountRate", 0)),
                    "tax_code": detail.get("TaxCodeRef", {}).get("value"),
                    "ingested_at": now_str,
                }

    return invoice_line_items


def _payments_resource(client: QuickBooksClient, since_date: date):
    @dlt.resource(
        name="payments",
        write_disposition="merge",
        merge_key="payment_id",
        primary_key="payment_id",
    )
    def payments():
        now_str = _now_utc_str()
        where = f"TxnDate >= '{since_date.isoformat()}'"
        raw = client.query("Payment", where=where, order_by="TxnDate DESC")
        logger.info(f"Fetched {len(raw)} payments since {since_date}")

        for pmt in raw:
            customer_ref = pmt.get("CustomerRef") or {}
            payment_method = pmt.get("PaymentMethodRef") or {}
            deposit_account = pmt.get("DepositToAccountRef") or {}

            yield {
                "payment_id": pmt["Id"],
                "txn_date": pmt.get("TxnDate"),
                "customer_id": customer_ref.get("value"),
                "customer_name": customer_ref.get("name"),
                "total_amount": float(pmt.get("TotalAmt", 0)),
                "unapplied_amount": float(pmt.get("UnappliedAmt", 0)),
                "payment_method_id": payment_method.get("value"),
                "payment_method_name": payment_method.get("name"),
                "deposit_to_account_id": deposit_account.get("value"),
                "deposit_to_account_name": deposit_account.get("name"),
                "payment_ref_num": pmt.get("PaymentRefNum"),
                "currency": pmt.get("CurrencyRef", {}).get("value", "USD"),
                "created_at": pmt.get("MetaData", {}).get("CreateTime"),
                "updated_at": pmt.get("MetaData", {}).get("LastUpdatedTime"),
                "ingested_at": now_str,
            }

    return payments


def _sales_receipts_resource(client: QuickBooksClient, since_date: date):
    @dlt.resource(
        name="sales_receipts",
        write_disposition="merge",
        merge_key="sales_receipt_id",
        primary_key="sales_receipt_id",
    )
    def sales_receipts():
        now_str = _now_utc_str()
        where = f"TxnDate >= '{since_date.isoformat()}'"
        raw = client.query("SalesReceipt", where=where, order_by="TxnDate DESC")
        logger.info(f"Fetched {len(raw)} sales receipts since {since_date}")

        for sr in raw:
            customer_ref = sr.get("CustomerRef") or {}
            payment_method = sr.get("PaymentMethodRef") or {}
            deposit_account = sr.get("DepositToAccountRef") or {}

            total_amount = float(sr.get("TotalAmt", 0))

            # Sum up line items for subtotal
            line_total = sum(
                float(line.get("Amount", 0))
                for line in sr.get("Line", [])
                if line.get("DetailType") == "SalesItemLineDetail"
            )

            yield {
                "sales_receipt_id": sr["Id"],
                "doc_number": sr.get("DocNumber"),
                "txn_date": sr.get("TxnDate"),
                "customer_id": customer_ref.get("value"),
                "customer_name": customer_ref.get("name"),
                "total_amount": total_amount,
                "subtotal_amount": line_total,
                "payment_method_id": payment_method.get("value"),
                "payment_method_name": payment_method.get("name"),
                "deposit_to_account_id": deposit_account.get("value"),
                "deposit_to_account_name": deposit_account.get("name"),
                "currency": sr.get("CurrencyRef", {}).get("value", "USD"),
                "memo": sr.get("PrivateNote"),
                "created_at": sr.get("MetaData", {}).get("CreateTime"),
                "updated_at": sr.get("MetaData", {}).get("LastUpdatedTime"),
                "ingested_at": now_str,
            }

    return sales_receipts


def _deposits_resource(client: QuickBooksClient, since_date: date):
    @dlt.resource(
        name="deposits",
        write_disposition="merge",
        merge_key="deposit_id",
        primary_key="deposit_id",
    )
    def deposits():
        now_str = _now_utc_str()
        where = f"TxnDate >= '{since_date.isoformat()}'"
        raw = client.query("Deposit", where=where, order_by="TxnDate DESC")
        logger.info(f"Fetched {len(raw)} deposits since {since_date}")

        for dep in raw:
            deposit_account = dep.get("DepositToAccountRef") or {}

            # Parse deposit line details for source info
            lines = []
            for line in dep.get("Line", []):
                detail = line.get("DepositLineDetail") or {}
                account_ref = detail.get("AccountRef") or {}
                entity_ref = detail.get("Entity") or {}
                lines.append({
                    "amount": float(line.get("Amount", 0)),
                    "account_name": account_ref.get("name"),
                    "entity_name": entity_ref.get("name"),
                    "memo": line.get("Description", ""),
                })

            # Summarize source info from lines — use entity name first, then
            # the per-line description (visible in QBO UI), then account name
            sources = ", ".join(
                f"{l['entity_name'] or l['memo'] or l['account_name'] or 'unknown'}"
                for l in lines if l["amount"] > 0
            )

            yield {
                "deposit_id": dep["Id"],
                "txn_date": dep.get("TxnDate"),
                "total_amount": float(dep.get("TotalAmt", 0)),
                "deposit_to_account_id": deposit_account.get("value"),
                "deposit_to_account_name": deposit_account.get("name"),
                "currency": dep.get("CurrencyRef", {}).get("value", "USD"),
                "memo": dep.get("PrivateNote"),
                "source_summary": sources,
                "line_count": len(lines),
                "created_at": dep.get("MetaData", {}).get("CreateTime"),
                "updated_at": dep.get("MetaData", {}).get("LastUpdatedTime"),
                "ingested_at": now_str,
            }

    return deposits


def _purchases_resource(client: QuickBooksClient, since_date: date):
    @dlt.resource(
        name="purchases",
        write_disposition="merge",
        merge_key="purchase_id",
        primary_key="purchase_id",
    )
    def purchases():
        now_str = _now_utc_str()
        where = f"TxnDate >= '{since_date.isoformat()}'"
        raw = client.query("Purchase", where=where, order_by="TxnDate DESC")
        logger.info(f"Fetched {len(raw)} purchases since {since_date}")

        for p in raw:
            account_ref = p.get("AccountRef") or {}
            vendor_ref = p.get("EntityRef") or {}

            # Summarize line items for description
            line_descriptions = []
            total_lines = 0
            for line in p.get("Line", []):
                total_lines += 1
                detail = line.get("AccountBasedExpenseLineDetail") or {}
                acct = detail.get("AccountRef") or {}
                desc = line.get("Description") or acct.get("name") or ""
                if desc:
                    line_descriptions.append(desc)

            yield {
                "purchase_id": p["Id"],
                "txn_date": p.get("TxnDate"),
                "payment_type": p.get("PaymentType"),
                "total_amount": float(p.get("TotalAmt", 0)),
                "account_id": account_ref.get("value"),
                "account_name": account_ref.get("name"),
                "vendor_id": vendor_ref.get("value"),
                "vendor_name": vendor_ref.get("name"),
                "entity_type": vendor_ref.get("type"),
                "doc_number": p.get("DocNumber"),
                "memo": p.get("PrivateNote"),
                "currency": p.get("CurrencyRef", {}).get("value", "USD"),
                "credit": p.get("Credit", False),
                "line_count": total_lines,
                "line_summary": "; ".join(line_descriptions[:5]),
                "created_at": p.get("MetaData", {}).get("CreateTime"),
                "updated_at": p.get("MetaData", {}).get("LastUpdatedTime"),
                "ingested_at": now_str,
            }

    return purchases


def _purchase_line_items_resource(client: QuickBooksClient, since_date: date):
    @dlt.resource(
        name="purchase_line_items",
        write_disposition="merge",
        merge_key="line_item_id",
        primary_key="line_item_id",
    )
    def purchase_line_items():
        now_str = _now_utc_str()
        where = f"TxnDate >= '{since_date.isoformat()}'"
        raw = client.query("Purchase", where=where, order_by="TxnDate DESC")

        for p in raw:
            purchase_id = p["Id"]
            txn_date = p.get("TxnDate")
            vendor_ref = p.get("EntityRef") or {}

            for line in p.get("Line", []):
                detail = line.get("AccountBasedExpenseLineDetail") or {}
                account_ref = detail.get("AccountRef") or {}
                customer_ref = detail.get("CustomerRef") or {}

                yield {
                    "line_item_id": f"{purchase_id}_{line.get('Id', '0')}",
                    "purchase_id": purchase_id,
                    "txn_date": txn_date,
                    "vendor_id": vendor_ref.get("value"),
                    "vendor_name": vendor_ref.get("name"),
                    "line_num": line.get("LineNum"),
                    "description": line.get("Description"),
                    "amount": float(line.get("Amount", 0)),
                    "account_id": account_ref.get("value"),
                    "account_name": account_ref.get("name"),
                    "customer_id": customer_ref.get("value"),
                    "customer_name": customer_ref.get("name"),
                    "billable_status": detail.get("BillableStatus"),
                    "ingested_at": now_str,
                }

    return purchase_line_items


def _bills_resource(client: QuickBooksClient, since_date: date):
    @dlt.resource(
        name="bills",
        write_disposition="merge",
        merge_key="bill_id",
        primary_key="bill_id",
    )
    def bills():
        now_str = _now_utc_str()
        where = f"TxnDate >= '{since_date.isoformat()}'"
        raw = client.query("Bill", where=where, order_by="TxnDate DESC")
        logger.info(f"Fetched {len(raw)} bills since {since_date}")

        for b in raw:
            vendor_ref = b.get("VendorRef") or {}

            # Summarize line items
            line_descriptions = []
            for line in b.get("Line", []):
                detail = line.get("AccountBasedExpenseLineDetail") or {}
                acct = detail.get("AccountRef") or {}
                desc = line.get("Description") or acct.get("name") or ""
                if desc:
                    line_descriptions.append(desc)

            yield {
                "bill_id": b["Id"],
                "txn_date": b.get("TxnDate"),
                "due_date": b.get("DueDate"),
                "vendor_id": vendor_ref.get("value"),
                "vendor_name": vendor_ref.get("name"),
                "total_amount": float(b.get("TotalAmt", 0)),
                "balance": float(b.get("Balance", 0)),
                "doc_number": b.get("DocNumber"),
                "memo": b.get("PrivateNote"),
                "currency": b.get("CurrencyRef", {}).get("value", "USD"),
                "line_summary": "; ".join(line_descriptions[:5]),
                "created_at": b.get("MetaData", {}).get("CreateTime"),
                "updated_at": b.get("MetaData", {}).get("LastUpdatedTime"),
                "ingested_at": now_str,
            }

    return bills


def _bill_payments_resource(client: QuickBooksClient, since_date: date):
    @dlt.resource(
        name="bill_payments",
        write_disposition="merge",
        merge_key="bill_payment_id",
        primary_key="bill_payment_id",
    )
    def bill_payments():
        now_str = _now_utc_str()
        where = f"TxnDate >= '{since_date.isoformat()}'"
        raw = client.query("BillPayment", where=where, order_by="TxnDate DESC")
        logger.info(f"Fetched {len(raw)} bill payments since {since_date}")

        for bp in raw:
            vendor_ref = bp.get("VendorRef") or {}
            check_detail = bp.get("CheckPayment") or {}
            cc_detail = bp.get("CreditCardPayment") or {}
            bank_account = (
                check_detail.get("BankAccountRef")
                or cc_detail.get("CCAccountRef")
                or {}
            )

            yield {
                "bill_payment_id": bp["Id"],
                "txn_date": bp.get("TxnDate"),
                "vendor_id": vendor_ref.get("value"),
                "vendor_name": vendor_ref.get("name"),
                "total_amount": float(bp.get("TotalAmt", 0)),
                "pay_type": bp.get("PayType"),
                "bank_account_id": bank_account.get("value"),
                "bank_account_name": bank_account.get("name"),
                "doc_number": bp.get("DocNumber"),
                "currency": bp.get("CurrencyRef", {}).get("value", "USD"),
                "created_at": bp.get("MetaData", {}).get("CreateTime"),
                "updated_at": bp.get("MetaData", {}).get("LastUpdatedTime"),
                "ingested_at": now_str,
            }

    return bill_payments


def _vendors_resource(client: QuickBooksClient):
    @dlt.resource(
        name="vendors",
        write_disposition="replace",
    )
    def vendors():
        now_str = _now_utc_str()
        raw = client.query("Vendor")
        logger.info(f"Fetched {len(raw)} vendors")

        for v in raw:
            primary_phone = v.get("PrimaryPhone") or {}
            primary_email = v.get("PrimaryEmailAddr") or {}

            yield {
                "vendor_id": v["Id"],
                "display_name": v.get("DisplayName"),
                "company_name": v.get("CompanyName"),
                "given_name": v.get("GivenName"),
                "family_name": v.get("FamilyName"),
                "email": primary_email.get("Address"),
                "phone": primary_phone.get("FreeFormNumber"),
                "balance": float(v.get("Balance", 0)),
                "active": v.get("Active", True),
                "vendor_1099": v.get("Vendor1099", False),
                "created_at": v.get("MetaData", {}).get("CreateTime"),
                "updated_at": v.get("MetaData", {}).get("LastUpdatedTime"),
                "ingested_at": now_str,
            }

    return vendors


def _customers_resource(client: QuickBooksClient):
    @dlt.resource(
        name="customers",
        write_disposition="replace",
    )
    def customers():
        now_str = _now_utc_str()
        raw = client.query("Customer")
        logger.info(f"Fetched {len(raw)} customers")

        for c in raw:
            primary_phone = c.get("PrimaryPhone") or {}
            primary_email = c.get("PrimaryEmailAddr") or {}
            bill_addr = c.get("BillAddr") or {}

            yield {
                "customer_id": c["Id"],
                "display_name": c.get("DisplayName"),
                "company_name": c.get("CompanyName"),
                "given_name": c.get("GivenName"),
                "family_name": c.get("FamilyName"),
                "email": primary_email.get("Address"),
                "phone": primary_phone.get("FreeFormNumber"),
                "city": bill_addr.get("City"),
                "state": bill_addr.get("CountrySubDivisionCode"),
                "postal_code": bill_addr.get("PostalCode"),
                "country": bill_addr.get("Country"),
                "balance": float(c.get("Balance", 0)),
                "active": c.get("Active", True),
                "is_project": c.get("IsProject", False),
                "created_at": c.get("MetaData", {}).get("CreateTime"),
                "updated_at": c.get("MetaData", {}).get("LastUpdatedTime"),
                "ingested_at": now_str,
            }

    return customers


def _accounts_resource(client: QuickBooksClient):
    @dlt.resource(
        name="accounts",
        write_disposition="replace",
    )
    def accounts():
        now_str = _now_utc_str()
        raw = client.query("Account")
        logger.info(f"Fetched {len(raw)} accounts")

        for a in raw:
            yield {
                "account_id": a["Id"],
                "name": a.get("Name"),
                "fully_qualified_name": a.get("FullyQualifiedName"),
                "account_type": a.get("AccountType"),
                "account_sub_type": a.get("AccountSubType"),
                "classification": a.get("Classification"),
                "current_balance": float(a.get("CurrentBalance", 0)),
                "currency": a.get("CurrencyRef", {}).get("value", "USD"),
                "active": a.get("Active", True),
                "created_at": a.get("MetaData", {}).get("CreateTime"),
                "updated_at": a.get("MetaData", {}).get("LastUpdatedTime"),
                "ingested_at": now_str,
            }

    return accounts


def _items_resource(client: QuickBooksClient):
    @dlt.resource(
        name="items",
        write_disposition="replace",
    )
    def items():
        now_str = _now_utc_str()
        raw = client.query("Item")
        logger.info(f"Fetched {len(raw)} items")

        for item in raw:
            income_acct = item.get("IncomeAccountRef") or {}
            expense_acct = item.get("ExpenseAccountRef") or {}

            yield {
                "item_id": item["Id"],
                "name": item.get("Name"),
                "fully_qualified_name": item.get("FullyQualifiedName"),
                "type": item.get("Type"),
                "description": item.get("Description"),
                "unit_price": float(item.get("UnitPrice", 0)),
                "purchase_cost": float(item.get("PurchaseCost", 0)),
                "sku": item.get("Sku"),
                "quantity_on_hand": float(item.get("QtyOnHand", 0)) if item.get("QtyOnHand") else None,
                "income_account_id": income_acct.get("value"),
                "income_account_name": income_acct.get("name"),
                "expense_account_id": expense_acct.get("value"),
                "expense_account_name": expense_acct.get("name"),
                "active": item.get("Active", True),
                "taxable": item.get("Taxable", False),
                "created_at": item.get("MetaData", {}).get("CreateTime"),
                "updated_at": item.get("MetaData", {}).get("LastUpdatedTime"),
                "ingested_at": now_str,
            }

    return items

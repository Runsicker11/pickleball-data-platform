"""Tests for Amazon Ads helpers."""

from pipelines.amazon_ads.helpers import normalize_row


def test_normalize_row_coerces_types():
    row = {
        "date": "2026-03-01",
        "campaignId": "123",
        "impressions": "1000",
        "clicks": "50",
        "cost": "12.34",
        "sales14d": "100.00",
    }
    result = normalize_row(row, profile_id="999")

    assert result["profile_id"] == "999"
    assert result["impressions"] == 1000
    assert result["clicks"] == 50
    assert result["cost"] == 12.34
    assert result["sales14d"] == 100.0
    assert "_loaded_at" in result


def test_normalize_row_handles_none():
    row = {
        "date": "2026-03-01",
        "impressions": None,
        "cost": None,
    }
    result = normalize_row(row, profile_id="999")

    assert result["impressions"] is None
    assert result["cost"] is None


def test_normalize_row_handles_bad_values():
    row = {
        "impressions": "not_a_number",
        "cost": "bad",
    }
    result = normalize_row(row, profile_id="999")

    assert result["impressions"] is None
    assert result["cost"] is None

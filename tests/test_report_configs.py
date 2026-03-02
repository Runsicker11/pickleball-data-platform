"""Tests for report config definitions."""

from pipelines.amazon_ads.report_configs import (
    ALL_REPORTS,
    ASIN_REPORTS,
    CAMPAIGN_REPORTS,
    SD_ADVERTISED_PRODUCTS,
    SP_CAMPAIGNS,
)


def test_all_reports_has_five():
    assert len(ALL_REPORTS) == 5


def test_campaign_reports_has_three():
    assert len(CAMPAIGN_REPORTS) == 3


def test_asin_reports_has_two():
    assert len(ASIN_REPORTS) == 2


def test_sp_campaigns_columns_include_date():
    assert "date" in SP_CAMPAIGNS.columns
    assert "campaignId" in SP_CAMPAIGNS.columns


def test_merge_keys_not_empty():
    for report in ALL_REPORTS:
        assert len(report.merge_keys) > 0, f"{report.name} has no merge keys"


def test_sd_asin_uses_promoted_asin():
    assert "promotedAsin" in SD_ADVERTISED_PRODUCTS.columns
    assert "promotedAsin" in SD_ADVERTISED_PRODUCTS.merge_keys

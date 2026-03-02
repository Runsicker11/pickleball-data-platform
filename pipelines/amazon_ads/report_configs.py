"""
Report type definitions for Amazon Ads.

Each config defines the API parameters needed to request a specific report type
and the merge keys used by dlt for deduplication.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ReportConfig:
    """Configuration for a single Amazon Ads report type."""

    name: str  # dlt resource / BQ table name
    ad_product: str  # SPONSORED_PRODUCTS, SPONSORED_BRANDS, SPONSORED_DISPLAY
    report_type_id: str  # spCampaigns, sbCampaigns, sdCampaigns, spAdvertisedProduct, etc.
    columns: list[str] = field(default_factory=list)
    group_by: list[str] = field(default_factory=lambda: ["campaign"])
    merge_keys: list[str] = field(default_factory=list)


# ── Campaign-level reports ───────────────────────────────────────────

SP_CAMPAIGNS = ReportConfig(
    name="sp_campaigns",
    ad_product="SPONSORED_PRODUCTS",
    report_type_id="spCampaigns",
    columns=[
        "date",
        "campaignId",
        "campaignName",
        "campaignStatus",
        "impressions",
        "clicks",
        "cost",
        "purchases14d",
        "sales14d",
        "unitsSoldClicks14d",
        "clickThroughRate",
        "costPerClick",
    ],
    group_by=["campaign"],
    merge_keys=["date", "campaignId"],
)

SB_CAMPAIGNS = ReportConfig(
    name="sb_campaigns",
    ad_product="SPONSORED_BRANDS",
    report_type_id="sbCampaigns",
    columns=[
        "date",
        "campaignId",
        "campaignName",
        "campaignStatus",
        "impressions",
        "clicks",
        "cost",
        "purchases",
        "sales",
        "unitsSold",
    ],
    group_by=["campaign"],
    merge_keys=["date", "campaignId"],
)

SD_CAMPAIGNS = ReportConfig(
    name="sd_campaigns",
    ad_product="SPONSORED_DISPLAY",
    report_type_id="sdCampaigns",
    columns=[
        "date",
        "campaignId",
        "campaignName",
        "campaignStatus",
        "impressions",
        "clicks",
        "cost",
        "purchasesClicks",
        "salesClicks",
        "unitsSoldClicks",
    ],
    group_by=["campaign"],
    merge_keys=["date", "campaignId"],
)

# ── ASIN-level reports ───────────────────────────────────────────────

SP_ADVERTISED_PRODUCTS = ReportConfig(
    name="sp_advertised_products",
    ad_product="SPONSORED_PRODUCTS",
    report_type_id="spAdvertisedProduct",
    columns=[
        "date",
        "campaignName",
        "campaignId",
        "adGroupName",
        "adGroupId",
        "advertisedAsin",
        "advertisedSku",
        "impressions",
        "clicks",
        "cost",
        "sales14d",
    ],
    group_by=["advertiser"],
    merge_keys=["date", "campaignId", "adGroupId", "advertisedAsin"],
)

SD_ADVERTISED_PRODUCTS = ReportConfig(
    name="sd_advertised_products",
    ad_product="SPONSORED_DISPLAY",
    report_type_id="sdAdvertisedProduct",
    columns=[
        "date",
        "campaignName",
        "campaignId",
        "adGroupName",
        "adGroupId",
        "promotedAsin",
        "promotedSku",
        "impressions",
        "clicks",
        "cost",
        "salesClicks",
        "purchasesClicks",
        "unitsSoldClicks",
    ],
    group_by=["advertiser"],
    merge_keys=["date", "campaignId", "adGroupId", "promotedAsin"],
)

# All reports to run by default
ALL_REPORTS: list[ReportConfig] = [
    SP_CAMPAIGNS,
    SB_CAMPAIGNS,
    SD_CAMPAIGNS,
    SP_ADVERTISED_PRODUCTS,
    SD_ADVERTISED_PRODUCTS,
]

CAMPAIGN_REPORTS: list[ReportConfig] = [SP_CAMPAIGNS, SB_CAMPAIGNS, SD_CAMPAIGNS]
ASIN_REPORTS: list[ReportConfig] = [SP_ADVERTISED_PRODUCTS, SD_ADVERTISED_PRODUCTS]

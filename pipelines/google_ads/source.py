"""dlt source for Google Ads — 5 resources: campaigns, ad_groups, keywords, daily_insights, search_terms."""

import logging
from datetime import date, timedelta

import dlt

from .client import GoogleAdsApiClient
from .helpers import micros_to_dollars, now_utc_str

logger = logging.getLogger(__name__)


@dlt.source(name="google_ads")
def google_ads_source(
    customer_id: str = dlt.secrets.value,
    developer_token: str = dlt.secrets.value,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    login_customer_id: str = "",
    days_back: int = 3,
):
    """dlt source yielding 5 Google Ads resources."""
    client = GoogleAdsApiClient(
        customer_id=customer_id,
        developer_token=developer_token,
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        login_customer_id=login_customer_id,
    )
    client.validate_access()

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back - 1)

    yield _campaigns_resource(client)
    yield _ad_groups_resource(client)
    yield _keywords_resource(client)
    yield _daily_insights_resource(client, start_date, end_date)
    yield _search_terms_resource(client, start_date, end_date)


def _campaigns_resource(client: GoogleAdsApiClient):
    @dlt.resource(name="campaigns", write_disposition="replace")
    def campaigns():
        ingested = now_utc_str()

        query = """
            SELECT
                campaign.id,
                campaign.name,
                campaign.advertising_channel_type,
                campaign.bidding_strategy_type,
                campaign.status,
                campaign.campaign_budget
            FROM campaign
            WHERE campaign.status != 'REMOVED'
        """
        rows = client.query(query)

        # Resolve budget amounts
        budget_cache = {}
        for row in rows:
            budget_rn = row.campaign.campaign_budget
            if budget_rn and budget_rn not in budget_cache:
                try:
                    budget_query = f"""
                        SELECT campaign_budget.amount_micros
                        FROM campaign_budget
                        WHERE campaign_budget.resource_name = '{budget_rn}'
                    """
                    budget_rows = client.query(budget_query)
                    if budget_rows:
                        budget_cache[budget_rn] = budget_rows[0].campaign_budget.amount_micros / 1_000_000
                    else:
                        budget_cache[budget_rn] = None
                except Exception:
                    budget_cache[budget_rn] = None

        for row in rows:
            yield {
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "campaign_type": row.campaign.advertising_channel_type.name,
                "bidding_strategy_type": row.campaign.bidding_strategy_type.name,
                "status": row.campaign.status.name,
                "budget_amount": budget_cache.get(row.campaign.campaign_budget),
                "ingested_at": ingested,
            }

    return campaigns


def _ad_groups_resource(client: GoogleAdsApiClient):
    @dlt.resource(name="ad_groups", write_disposition="replace")
    def ad_groups():
        ingested = now_utc_str()
        query = """
            SELECT
                ad_group.id,
                ad_group.name,
                ad_group.campaign,
                ad_group.type,
                ad_group.status,
                ad_group.cpc_bid_micros,
                campaign.id,
                campaign.name
            FROM ad_group
            WHERE ad_group.status != 'REMOVED'
        """
        for row in client.query(query):
            cpc_bid = None
            if row.ad_group.cpc_bid_micros:
                cpc_bid = row.ad_group.cpc_bid_micros / 1_000_000

            yield {
                "ad_group_id": row.ad_group.id,
                "ad_group_name": row.ad_group.name,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "ad_group_type": row.ad_group.type_.name,
                "status": row.ad_group.status.name,
                "cpc_bid_micros": cpc_bid,
                "ingested_at": ingested,
            }

    return ad_groups


def _keywords_resource(client: GoogleAdsApiClient):
    @dlt.resource(name="keywords", write_disposition="replace")
    def keywords():
        ingested = now_utc_str()
        query = """
            SELECT
                ad_group_criterion.criterion_id,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.status,
                ad_group_criterion.quality_info.quality_score,
                ad_group_criterion.quality_info.creative_quality_score,
                ad_group_criterion.quality_info.post_click_quality_score,
                ad_group_criterion.quality_info.search_predicted_ctr,
                ad_group.id,
                campaign.id
            FROM keyword_view
            WHERE ad_group_criterion.status != 'REMOVED'
        """
        for row in client.query(query):
            quality_score = row.ad_group_criterion.quality_info.quality_score
            if quality_score == 0:
                quality_score = None

            yield {
                "keyword_id": row.ad_group_criterion.criterion_id,
                "keyword_text": row.ad_group_criterion.keyword.text,
                "match_type": row.ad_group_criterion.keyword.match_type.name,
                "ad_group_id": row.ad_group.id,
                "campaign_id": row.campaign.id,
                "status": row.ad_group_criterion.status.name,
                "quality_score": quality_score,
                "expected_ctr": row.ad_group_criterion.quality_info.search_predicted_ctr.name,
                "ad_relevance": row.ad_group_criterion.quality_info.creative_quality_score.name,
                "landing_page_experience": row.ad_group_criterion.quality_info.post_click_quality_score.name,
                "ingested_at": ingested,
            }

    return keywords


def _daily_insights_resource(client: GoogleAdsApiClient, start_date: date, end_date: date):
    @dlt.resource(
        name="daily_insights",
        write_disposition="merge",
        merge_key=["date_start", "campaign_id", "ad_group_id"],
        primary_key=["date_start", "campaign_id", "ad_group_id"],
    )
    def daily_insights():
        ingested = now_utc_str()
        query = f"""
            SELECT
                segments.date,
                campaign.id,
                campaign.name,
                campaign.advertising_channel_type,
                ad_group.id,
                ad_group.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.average_cpc,
                metrics.ctr,
                metrics.conversions,
                metrics.conversions_value,
                metrics.cost_per_conversion,
                metrics.search_impression_share
            FROM ad_group
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND metrics.impressions > 0
        """
        for row in client.query(query):
            cost_per_conv = micros_to_dollars(row.metrics.cost_per_conversion)
            search_is = row.metrics.search_impression_share or None

            yield {
                "date_start": row.segments.date,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "campaign_type": row.campaign.advertising_channel_type.name,
                "ad_group_id": row.ad_group.id,
                "ad_group_name": row.ad_group.name,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "spend": row.metrics.cost_micros / 1_000_000,
                "cpc": row.metrics.average_cpc / 1_000_000,
                "ctr": row.metrics.ctr,
                "conversions": row.metrics.conversions,
                "conversion_value": row.metrics.conversions_value,
                "cost_per_conversion": cost_per_conv,
                "search_impression_share": search_is,
                "ingested_at": ingested,
            }

    return daily_insights


def _search_terms_resource(client: GoogleAdsApiClient, start_date: date, end_date: date):
    @dlt.resource(
        name="search_terms",
        write_disposition="merge",
        merge_key=["date_start", "search_term", "campaign_id", "ad_group_id"],
        primary_key=["date_start", "search_term", "campaign_id", "ad_group_id"],
    )
    def search_terms():
        ingested = now_utc_str()
        query = f"""
            SELECT
                segments.date,
                search_term_view.search_term,
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                search_term_view.status,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value
            FROM search_term_view
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND metrics.impressions > 0
        """
        for row in client.query(query):
            yield {
                "date_start": row.segments.date,
                "search_term": row.search_term_view.search_term,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "ad_group_id": row.ad_group.id,
                "ad_group_name": row.ad_group.name,
                "keyword_text": "",
                "match_type": "",
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "spend": row.metrics.cost_micros / 1_000_000,
                "conversions": row.metrics.conversions,
                "conversion_value": row.metrics.conversions_value,
                "ingested_at": ingested,
            }

    return search_terms

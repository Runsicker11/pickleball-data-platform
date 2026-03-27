"""dlt source for Google Ads — 17 resources: campaigns, ad_groups, keywords, daily_insights, search_terms, bidding_strategy, conversion_action, shopping_performance, asset_group, asset_group_asset, campaign_asset_set, geographic_view, campaign_audience_view, ad_schedule_view, campaign_negative_keywords, ad_group_negative_keywords, pmax_insights."""

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
    """dlt source yielding 17 Google Ads resources."""
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
    start_date_90d = date.today() - timedelta(days=91)

    yield _campaigns_resource(client)
    yield _ad_groups_resource(client)
    yield _keywords_resource(client)
    yield _daily_insights_resource(client, start_date, end_date)
    yield _search_terms_resource(client, start_date, end_date)
    yield _bidding_strategy_resource(client)
    yield _conversion_action_resource(client)
    yield _shopping_performance_resource(client, start_date_90d, end_date)
    yield _asset_group_resource(client)
    yield _asset_group_asset_resource(client)
    yield _campaign_asset_set_resource(client)
    yield _geographic_view_resource(client, start_date_90d, end_date)
    yield _campaign_audience_view_resource(client)
    yield _ad_schedule_view_resource(client, start_date_90d, end_date)
    yield _campaign_negative_keywords_resource(client)
    yield _ad_group_negative_keywords_resource(client)
    yield _pmax_insights_resource(client, start_date, end_date)


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


def _bidding_strategy_resource(client: GoogleAdsApiClient):
    @dlt.resource(name="bidding_strategy", write_disposition="replace")
    def bidding_strategy():
        ingested = now_utc_str()
        # campaign fields are not selectable from the bidding_strategy resource
        query = """
            SELECT
                bidding_strategy.id,
                bidding_strategy.name,
                bidding_strategy.type,
                bidding_strategy.target_cpa.target_cpa_micros,
                bidding_strategy.target_roas.target_roas,
                bidding_strategy.maximize_conversions.target_cpa_micros
            FROM bidding_strategy
        """
        for row in client.query(query):
            yield {
                "bidding_strategy_id": row.bidding_strategy.id,
                "bidding_strategy_name": row.bidding_strategy.name,
                "bidding_strategy_type": row.bidding_strategy.type_.name,
                "target_cpa_micros": row.bidding_strategy.target_cpa.target_cpa_micros or None,
                "target_roas": row.bidding_strategy.target_roas.target_roas or None,
                "maximize_conversions_target_cpa_micros": row.bidding_strategy.maximize_conversions.target_cpa_micros or None,
                "ingested_at": ingested,
            }

    return bidding_strategy


def _conversion_action_resource(client: GoogleAdsApiClient):
    @dlt.resource(name="conversion_action", write_disposition="replace")
    def conversion_action():
        ingested = now_utc_str()
        query = """
            SELECT
                conversion_action.id,
                conversion_action.name,
                conversion_action.status,
                conversion_action.type,
                conversion_action.value_settings.default_value,
                conversion_action.value_settings.always_use_default_value,
                conversion_action.counting_type,
                conversion_action.include_in_conversions_metric
            FROM conversion_action
        """
        for row in client.query(query):
            yield {
                "conversion_action_id": row.conversion_action.id,
                "conversion_action_name": row.conversion_action.name,
                "status": row.conversion_action.status.name,
                "conversion_type": row.conversion_action.type_.name,
                "default_value": row.conversion_action.value_settings.default_value or None,
                "always_use_default_value": row.conversion_action.value_settings.always_use_default_value,
                "counting_type": row.conversion_action.counting_type.name,
                "include_in_conversions_metric": row.conversion_action.include_in_conversions_metric,
                "ingested_at": ingested,
            }

    return conversion_action


def _shopping_performance_resource(client: GoogleAdsApiClient, start_date: date, end_date: date):
    @dlt.resource(
        name="shopping_performance",
        write_disposition="merge",
        merge_key=["date_start", "product_item_id", "campaign_id"],
        primary_key=["date_start", "product_item_id", "campaign_id"],
    )
    def shopping_performance():
        ingested = now_utc_str()
        query = f"""
            SELECT
                segments.date,
                segments.product_title,
                segments.product_item_id,
                segments.product_type_l1,
                segments.product_brand,
                campaign.id,
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value
            FROM shopping_performance_view
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND metrics.impressions > 0
        """
        for row in client.query(query):
            yield {
                "date_start": row.segments.date,
                "product_title": row.segments.product_title,
                "product_item_id": row.segments.product_item_id,
                "product_type_l1": row.segments.product_type_l1,
                "product_brand": row.segments.product_brand,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "spend": row.metrics.cost_micros / 1_000_000,
                "conversions": row.metrics.conversions,
                "conversion_value": row.metrics.conversions_value,
                "ingested_at": ingested,
            }

    return shopping_performance


def _asset_group_resource(client: GoogleAdsApiClient):
    @dlt.resource(name="asset_group", write_disposition="replace")
    def asset_group():
        ingested = now_utc_str()
        query = """
            SELECT
                asset_group.id,
                asset_group.name,
                asset_group.status,
                asset_group.final_urls,
                campaign.id,
                campaign.name
            FROM asset_group
            WHERE asset_group.status != 'REMOVED'
        """
        for row in client.query(query):
            yield {
                "asset_group_id": row.asset_group.id,
                "asset_group_name": row.asset_group.name,
                "status": row.asset_group.status.name,
                "final_urls": list(row.asset_group.final_urls),
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "ingested_at": ingested,
            }

    return asset_group


def _asset_group_asset_resource(client: GoogleAdsApiClient):
    @dlt.resource(name="asset_group_asset", write_disposition="replace")
    def asset_group_asset():
        ingested = now_utc_str()
        query = """
            SELECT
                asset_group_asset.asset,
                asset_group_asset.field_type,
                asset_group_asset.status,
                asset_group.id,
                asset_group.name,
                campaign.id,
                campaign.name
            FROM asset_group_asset
            WHERE asset_group_asset.status != 'REMOVED'
        """
        for row in client.query(query):
            yield {
                "asset_resource_name": row.asset_group_asset.asset,
                "field_type": row.asset_group_asset.field_type.name,
                "status": row.asset_group_asset.status.name,
                "asset_group_id": row.asset_group.id,
                "asset_group_name": row.asset_group.name,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "ingested_at": ingested,
            }

    return asset_group_asset


def _campaign_asset_set_resource(client: GoogleAdsApiClient):
    @dlt.resource(name="campaign_asset_set", write_disposition="replace")
    def campaign_asset_set():
        ingested = now_utc_str()
        query = """
            SELECT
                campaign_asset_set.campaign,
                campaign_asset_set.asset_set,
                campaign_asset_set.status,
                asset_set.id,
                asset_set.name,
                asset_set.type,
                campaign.id,
                campaign.name
            FROM campaign_asset_set
            WHERE campaign_asset_set.status != 'REMOVED'
        """
        for row in client.query(query):
            yield {
                "campaign_resource_name": row.campaign_asset_set.campaign,
                "asset_set_resource_name": row.campaign_asset_set.asset_set,
                "status": row.campaign_asset_set.status.name,
                "asset_set_id": row.asset_set.id,
                "asset_set_name": row.asset_set.name,
                "asset_set_type": row.asset_set.type_.name,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "ingested_at": ingested,
            }

    return campaign_asset_set


def _geographic_view_resource(client: GoogleAdsApiClient, start_date: date, end_date: date):
    @dlt.resource(
        name="geographic_view",
        write_disposition="merge",
        merge_key=["date_start", "campaign_id", "country_criterion_id"],
        primary_key=["date_start", "campaign_id", "country_criterion_id"],
    )
    def geographic_view():
        ingested = now_utc_str()
        query = f"""
            SELECT
                geographic_view.location_type,
                geographic_view.country_criterion_id,
                campaign.id,
                campaign.name,
                segments.date,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value
            FROM geographic_view
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND metrics.impressions > 0
        """
        for row in client.query(query):
            yield {
                "date_start": row.segments.date,
                "location_type": row.geographic_view.location_type.name,
                "country_criterion_id": row.geographic_view.country_criterion_id,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "spend": row.metrics.cost_micros / 1_000_000,
                "conversions": row.metrics.conversions,
                "conversion_value": row.metrics.conversions_value,
                "ingested_at": ingested,
            }

    return geographic_view


def _campaign_audience_view_resource(client: GoogleAdsApiClient):
    @dlt.resource(name="campaign_audience_view", write_disposition="replace")
    def campaign_audience_view():
        ingested = now_utc_str()
        # ad_group_criterion fields are not selectable from campaign_audience_view
        query = """
            SELECT
                campaign_audience_view.resource_name,
                campaign.id,
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value
            FROM campaign_audience_view
        """
        for row in client.query(query):
            yield {
                "resource_name": row.campaign_audience_view.resource_name,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "spend": row.metrics.cost_micros / 1_000_000,
                "conversions": row.metrics.conversions,
                "conversion_value": row.metrics.conversions_value,
                "ingested_at": ingested,
            }

    return campaign_audience_view


def _auction_insights_resource(client: GoogleAdsApiClient, start_date: date, end_date: date):
    @dlt.resource(
        name="auction_insights",
        write_disposition="merge",
        merge_key=["date_start", "campaign_id", "domain"],
        primary_key=["date_start", "campaign_id", "domain"],
    )
    def auction_insights():
        ingested = now_utc_str()
        query = f"""
            SELECT
                segments.date,
                campaign.id,
                campaign.name,
                segments.auction_insight_domain,
                metrics.auction_insight_search_impression_share,
                metrics.auction_insight_search_overlap_rate,
                metrics.auction_insight_search_position_above_rate,
                metrics.auction_insight_search_top_impression_percentage,
                metrics.auction_insight_search_outranking_share
            FROM campaign
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND campaign.status != 'REMOVED'
        """
        try:
            rows = client.query(query)
        except Exception as exc:
            # Auction insight metrics require competitive intelligence API access.
            # Apply at: https://ads.google.com/intl/en_us/home/tools/manager-accounts/
            logger.warning(f"auction_insights unavailable (developer token lacks competitive metrics access): {exc}")
            return

        for row in rows:
            domain = row.segments.auction_insight_domain
            if not domain:
                continue
            yield {
                "date_start": row.segments.date,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "domain": domain,
                "impression_share": row.metrics.auction_insight_search_impression_share or None,
                "overlap_rate": row.metrics.auction_insight_search_overlap_rate or None,
                "position_above_rate": row.metrics.auction_insight_search_position_above_rate or None,
                "top_of_page_rate": row.metrics.auction_insight_search_top_impression_percentage or None,
                "outranking_share": row.metrics.auction_insight_search_outranking_share or None,
                "ingested_at": ingested,
            }

    return auction_insights


def _ad_schedule_view_resource(client: GoogleAdsApiClient, start_date: date, end_date: date):
    @dlt.resource(
        name="ad_schedule_view",
        write_disposition="merge",
        merge_key=["date_start", "campaign_id", "criterion_id"],
        primary_key=["date_start", "campaign_id", "criterion_id"],
    )
    def ad_schedule_view():
        ingested = now_utc_str()
        query = f"""
            SELECT
                ad_schedule_view.resource_name,
                campaign.id,
                campaign.name,
                campaign_criterion.ad_schedule.day_of_week,
                campaign_criterion.ad_schedule.start_hour,
                campaign_criterion.ad_schedule.end_hour,
                campaign_criterion.ad_schedule.start_minute,
                campaign_criterion.ad_schedule.end_minute,
                campaign_criterion.criterion_id,
                segments.date,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value
            FROM ad_schedule_view
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND metrics.impressions > 0
        """
        for row in client.query(query):
            yield {
                "date_start": row.segments.date,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "criterion_id": row.campaign_criterion.criterion_id,
                "day_of_week": row.campaign_criterion.ad_schedule.day_of_week.name,
                "start_hour": row.campaign_criterion.ad_schedule.start_hour,
                "end_hour": row.campaign_criterion.ad_schedule.end_hour,
                "start_minute": row.campaign_criterion.ad_schedule.start_minute.name,
                "end_minute": row.campaign_criterion.ad_schedule.end_minute.name,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "cost": row.metrics.cost_micros / 1_000_000,
                "conversions": row.metrics.conversions,
                "conversions_value": row.metrics.conversions_value,
                "ingested_at": ingested,
            }

    return ad_schedule_view


def _campaign_negative_keywords_resource(client: GoogleAdsApiClient):
    @dlt.resource(name="campaign_negative_keywords", write_disposition="replace")
    def campaign_negative_keywords():
        ingested = now_utc_str()
        query = """
            SELECT
                campaign_criterion.criterion_id,
                campaign_criterion.keyword.text,
                campaign_criterion.keyword.match_type,
                campaign_criterion.status,
                campaign.id,
                campaign.name
            FROM campaign_criterion
            WHERE campaign_criterion.type = 'KEYWORD'
              AND campaign_criterion.negative = TRUE
              AND campaign_criterion.status != 'REMOVED'
        """
        for row in client.query(query):
            yield {
                "criterion_id": row.campaign_criterion.criterion_id,
                "keyword_text": row.campaign_criterion.keyword.text,
                "match_type": row.campaign_criterion.keyword.match_type.name,
                "status": row.campaign_criterion.status.name,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "ingested_at": ingested,
            }

    return campaign_negative_keywords


def _ad_group_negative_keywords_resource(client: GoogleAdsApiClient):
    @dlt.resource(name="ad_group_negative_keywords", write_disposition="replace")
    def ad_group_negative_keywords():
        ingested = now_utc_str()
        query = """
            SELECT
                ad_group_criterion.criterion_id,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.status,
                ad_group.id,
                ad_group.name,
                campaign.id,
                campaign.name
            FROM ad_group_criterion
            WHERE ad_group_criterion.type = 'KEYWORD'
              AND ad_group_criterion.negative = TRUE
              AND ad_group_criterion.status != 'REMOVED'
        """
        for row in client.query(query):
            yield {
                "criterion_id": row.ad_group_criterion.criterion_id,
                "keyword_text": row.ad_group_criterion.keyword.text,
                "match_type": row.ad_group_criterion.keyword.match_type.name,
                "status": row.ad_group_criterion.status.name,
                "ad_group_id": row.ad_group.id,
                "ad_group_name": row.ad_group.name,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "ingested_at": ingested,
            }

    return ad_group_negative_keywords


def _pmax_insights_resource(client: GoogleAdsApiClient, start_date: date, end_date: date):
    @dlt.resource(
        name="pmax_insights",
        write_disposition="merge",
        merge_key=["date_start", "campaign_id"],
        primary_key=["date_start", "campaign_id"],
    )
    def pmax_insights():
        ingested = now_utc_str()
        # PMAX campaigns don't have ad groups — must query from campaign resource directly
        query = f"""
            SELECT
                segments.date,
                campaign.id,
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value
            FROM campaign
            WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
              AND campaign.status = 'ENABLED'
              AND segments.date BETWEEN '{start_date}' AND '{end_date}'
        """
        for row in client.query(query):
            yield {
                "date_start": row.segments.date,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "spend": row.metrics.cost_micros / 1_000_000,
                "conversions": row.metrics.conversions,
                "conversion_value": row.metrics.conversions_value,
                "ingested_at": ingested,
            }

    return pmax_insights

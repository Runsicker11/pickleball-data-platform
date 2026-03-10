-- Unified daily performance across Meta + Google Ads
-- Replaces: vw_daily_performance

with meta as (
    select
        date_start as report_date,
        'meta' as platform,
        campaign_id,
        campaign_name,
        cast(null as string) as campaign_type,
        adset_id,
        adset_name,
        ad_id,
        ad_name,
        impressions,
        clicks,
        link_clicks,
        spend,
        cpc,
        cpm,
        ctr,
        reach,
        frequency,
        purchases as conversions,
        purchase_value as conversion_value,
        add_to_cart,
        add_to_cart_value,
        initiate_checkout,
        initiate_checkout_value,
        landing_page_views
    from {{ ref('stg_meta__daily_insights') }}
),

google as (
    select
        date_start as report_date,
        'google_ads' as platform,
        cast(campaign_id as string) as campaign_id,
        campaign_name,
        campaign_type,
        cast(ad_group_id as string) as adset_id,
        ad_group_name as adset_name,
        cast(null as string) as ad_id,
        cast(null as string) as ad_name,
        impressions,
        clicks,
        cast(null as int64) as link_clicks,
        spend,
        cpc,
        cast(null as float64) as cpm,
        ctr,
        cast(null as int64) as reach,
        cast(null as float64) as frequency,
        conversions,
        conversion_value,
        cast(null as int64) as add_to_cart,
        cast(null as float64) as add_to_cart_value,
        cast(null as int64) as initiate_checkout,
        cast(null as float64) as initiate_checkout_value,
        cast(null as int64) as landing_page_views
    from {{ ref('stg_google_ads__daily_insights') }}
)

select * from meta
union all
select * from google

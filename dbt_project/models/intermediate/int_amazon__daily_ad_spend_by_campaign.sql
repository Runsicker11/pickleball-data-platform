-- Unified daily ad spend across all campaign types (SP + SB + SD)

with sp as (
    select
        report_date,
        campaign_id,
        campaign_name,
        campaign_status,
        profile_id,
        ad_product,
        impressions,
        clicks,
        cost,
        purchases,
        sales,
        units_sold,
        ctr,
        cpc,
        acos,
        roas
    from {{ ref('stg_amazon_ads__sp_campaigns') }}
),

sb as (
    select
        report_date,
        campaign_id,
        campaign_name,
        campaign_status,
        profile_id,
        ad_product,
        impressions,
        clicks,
        cost,
        purchases,
        sales,
        units_sold,
        ctr,
        cpc,
        acos,
        roas
    from {{ ref('stg_amazon_ads__sb_campaigns') }}
),

sd as (
    select
        report_date,
        campaign_id,
        campaign_name,
        campaign_status,
        profile_id,
        ad_product,
        impressions,
        clicks,
        cost,
        purchases,
        sales,
        units_sold,
        ctr,
        cpc,
        acos,
        roas
    from {{ ref('stg_amazon_ads__sd_campaigns') }}
),

unioned as (
    select * from sp
    union all
    select * from sb
    union all
    select * from sd
)

select * from unioned

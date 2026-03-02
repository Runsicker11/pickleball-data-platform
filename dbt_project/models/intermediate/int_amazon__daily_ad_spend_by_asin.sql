-- Unified daily ad spend by ASIN across SP + SD

with sp as (
    select
        report_date,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        asin,
        sku,
        profile_id,
        ad_product,
        impressions,
        clicks,
        cost,
        sales,
        ctr,
        cpc,
        acos,
        roas
    from {{ ref('stg_amazon_ads__sp_asin') }}
),

sd as (
    select
        report_date,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        asin,
        sku,
        profile_id,
        ad_product,
        impressions,
        clicks,
        cost,
        sales,
        ctr,
        cpc,
        acos,
        roas
    from {{ ref('stg_amazon_ads__sd_asin') }}
),

unioned as (
    select * from sp
    union all
    select * from sd
)

select * from unioned

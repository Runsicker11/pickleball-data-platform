-- Daily rollup by channel
-- Replaces: vw_channel_summary

with meta_daily as (
    select
        date_start as report_date,
        'meta' as channel,
        sum(spend) as spend,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(purchases) as conversions,
        sum(purchase_value) as conversion_value
    from {{ ref('stg_meta__daily_insights') }}
    group by date_start
),

google_daily as (
    select
        date_start as report_date,
        'google_ads' as channel,
        sum(spend) as spend,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(conversions) as conversions,
        sum(conversion_value) as conversion_value
    from {{ ref('stg_google_ads__daily_insights') }}
    group by date_start
),

ga4_organic as (
    select
        order_date as report_date,
        'organic' as channel,
        cast(null as float64) as spend,
        cast(null as int64) as impressions,
        cast(null as int64) as clicks,
        cast(count(distinct order_id) as float64) as conversions,
        sum(shopify_revenue) as conversion_value
    from {{ ref('int_marketing__ga4_attribution') }}
    where lower(coalesce(ga4_medium, '')) = 'organic'
        and shopify_revenue is not null
    group by order_date
),

ga4_direct as (
    select
        order_date as report_date,
        'direct' as channel,
        cast(null as float64) as spend,
        cast(null as int64) as impressions,
        cast(null as int64) as clicks,
        cast(count(distinct order_id) as float64) as conversions,
        sum(shopify_revenue) as conversion_value
    from {{ ref('int_marketing__ga4_attribution') }}
    where (lower(coalesce(ga4_source, '')) = '(direct)' or ga4_source is null)
        and shopify_revenue is not null
    group by order_date
),

ga4_referral as (
    select
        order_date as report_date,
        'referral' as channel,
        cast(null as float64) as spend,
        cast(null as int64) as impressions,
        cast(null as int64) as clicks,
        cast(count(distinct order_id) as float64) as conversions,
        sum(shopify_revenue) as conversion_value
    from {{ ref('int_marketing__ga4_attribution') }}
    where lower(coalesce(ga4_medium, '')) = 'referral'
        and shopify_revenue is not null
    group by order_date
),

all_channels as (
    select * from meta_daily
    union all select * from google_daily
    union all select * from ga4_organic
    union all select * from ga4_direct
    union all select * from ga4_referral
)

select
    report_date,
    channel,
    spend,
    impressions,
    clicks,
    conversions,
    conversion_value as revenue,
    safe_divide(conversion_value, spend) as roas,
    safe_divide(spend, conversions) as cpa,
    cast(conversions as int64) as orders
from all_channels

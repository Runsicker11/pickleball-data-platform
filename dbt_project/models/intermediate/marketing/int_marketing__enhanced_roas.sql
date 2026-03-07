-- GA4-attributed ROAS by channel
-- Replaces: vw_enhanced_roas

with ga4_revenue as (
    select
        order_date as report_date,
        case
            when lower(coalesce(ga4_source, '')) in ('facebook', 'fb', 'ig', 'instagram', 'meta')
                or lower(coalesce(ga4_medium, '')) in ('paid_social', 'paidsocial')
                then 'meta'
            when lower(coalesce(ga4_source, '')) = 'google'
                and lower(coalesce(ga4_medium, '')) in ('cpc', 'ppc', 'paid')
                then 'google_ads'
            when lower(coalesce(ga4_medium, '')) in ('organic', 'referral')
                then lower(ga4_medium)
            when lower(coalesce(ga4_source, '')) = '(direct)'
                or ga4_source is null
                then 'direct'
            else 'other'
        end as channel,
        shopify_revenue,
        order_id
    from {{ ref('int_marketing__ga4_attribution') }}
    where shopify_revenue is not null
),

channel_daily as (
    select
        report_date,
        channel,
        sum(shopify_revenue) as ga4_attributed_revenue,
        count(distinct order_id) as ga4_attributed_orders
    from ga4_revenue
    group by report_date, channel
),

meta_spend as (
    select
        date_start as report_date,
        sum(spend) as ad_spend
    from {{ ref('stg_meta__daily_insights') }}
    group by date_start
),

google_spend as (
    select
        date_start as report_date,
        sum(spend) as ad_spend
    from {{ ref('stg_google_ads__daily_insights') }}
    group by date_start
)

select
    c.report_date,
    c.channel,
    c.ga4_attributed_revenue,
    c.ga4_attributed_orders,
    case
        when c.channel = 'meta' then m.ad_spend
        when c.channel = 'google_ads' then g.ad_spend
        else null
    end as ad_spend,
    case
        when c.channel = 'meta' then safe_divide(c.ga4_attributed_revenue, m.ad_spend)
        when c.channel = 'google_ads' then safe_divide(c.ga4_attributed_revenue, g.ad_spend)
        else null
    end as enhanced_roas
from channel_daily c
left join meta_spend m on c.report_date = m.report_date and c.channel = 'meta'
left join google_spend g on c.report_date = g.report_date and c.channel = 'google_ads'

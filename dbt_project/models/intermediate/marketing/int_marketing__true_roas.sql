-- Cross-platform spend vs actual Shopify revenue (UTM-attributed)
-- Replaces: vw_true_roas

with meta_daily as (
    select
        date_start as report_date,
        sum(spend) as meta_spend,
        sum(impressions) as meta_impressions,
        sum(clicks) as meta_clicks,
        sum(purchases) as meta_reported_purchases,
        sum(purchase_value) as meta_reported_revenue
    from {{ ref('stg_meta__daily_insights') }}
    group by date_start
),

google_daily as (
    select
        date_start as report_date,
        sum(spend) as google_spend,
        sum(impressions) as google_impressions,
        sum(clicks) as google_clicks,
        sum(conversions) as google_conversions,
        sum(conversion_value) as google_conversion_value
    from {{ ref('stg_google_ads__daily_insights') }}
    group by date_start
),

shopify_meta as (
    select
        order_date as report_date,
        count(*) as shopify_orders,
        sum(total_price) as shopify_meta_revenue
    from {{ ref('stg_shopify__orders') }}
    where lower(coalesce(utm_source, '')) in ('facebook', 'fb', 'ig', 'instagram', 'meta')
        and financial_status not in ('refunded', 'voided')
    group by order_date
),

shopify_google as (
    select
        order_date as report_date,
        count(*) as shopify_google_orders,
        sum(total_price) as shopify_google_revenue
    from {{ ref('stg_shopify__orders') }}
    where lower(coalesce(utm_source, '')) = 'google'
        and lower(coalesce(utm_medium, '')) in ('cpc', 'ppc', 'paid')
        and financial_status not in ('refunded', 'voided')
    group by order_date
)

select
    coalesce(m.report_date, g.report_date, sm.report_date, sg.report_date) as report_date,
    m.meta_spend,
    m.meta_impressions,
    m.meta_clicks,
    m.meta_reported_purchases,
    m.meta_reported_revenue,
    sm.shopify_orders as shopify_meta_orders,
    sm.shopify_meta_revenue,
    safe_divide(sm.shopify_meta_revenue, m.meta_spend) as meta_true_roas,
    safe_divide(m.meta_reported_revenue, m.meta_spend) as meta_reported_roas,
    coalesce(m.meta_reported_revenue, 0) - coalesce(sm.shopify_meta_revenue, 0) as revenue_gap,
    g.google_spend,
    g.google_impressions,
    g.google_clicks,
    g.google_conversions,
    g.google_conversion_value,
    sg.shopify_google_orders,
    sg.shopify_google_revenue,
    safe_divide(sg.shopify_google_revenue, g.google_spend) as google_true_roas,
    safe_divide(
        coalesce(sm.shopify_meta_revenue, 0) + coalesce(sg.shopify_google_revenue, 0),
        coalesce(m.meta_spend, 0) + coalesce(g.google_spend, 0)
    ) as blended_true_roas,
    safe_divide(
        coalesce(sm.shopify_meta_revenue, 0) + coalesce(sg.shopify_google_revenue, 0),
        coalesce(m.meta_spend, 0) + coalesce(g.google_spend, 0)
    ) as true_roas
from meta_daily m
full outer join google_daily g on m.report_date = g.report_date
full outer join shopify_meta sm on coalesce(m.report_date, g.report_date) = sm.report_date
full outer join shopify_google sg on coalesce(m.report_date, g.report_date) = sg.report_date

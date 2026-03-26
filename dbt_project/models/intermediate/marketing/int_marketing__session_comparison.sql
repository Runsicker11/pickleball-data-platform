-- Disabled: depends on stg_shopify__sessions which requires Shopify Plus
{{ config(enabled=False) }}

-- Daily Shopify vs GA4 session/funnel comparison
-- Expected: GA4 captures ~80-95% of Shopify sessions (ad blockers, cookie consent, differing definitions)

with shopify_daily as (
    select
        report_date,
        sum(sessions) as shopify_sessions,
        sum(product_views) as shopify_product_views,
        sum(add_to_carts) as shopify_add_to_carts,
        sum(checkouts) as shopify_checkouts,
        sum(orders) as shopify_orders
    from {{ ref('stg_shopify__sessions') }}
    group by report_date
),

ga4_daily as (
    select
        report_date,
        sum(sessions) as ga4_sessions,
        sum(product_views) as ga4_product_views,
        sum(add_to_carts) as ga4_add_to_carts,
        sum(checkouts) as ga4_checkouts,
        sum(purchases) as ga4_purchases
    from {{ ref('int_marketing__ga4_funnel') }}
    group by report_date
)

select
    coalesce(s.report_date, g.report_date) as report_date,

    -- Sessions
    s.shopify_sessions,
    g.ga4_sessions,
    s.shopify_sessions - g.ga4_sessions as session_gap,
    safe_divide(s.shopify_sessions - g.ga4_sessions, s.shopify_sessions) as session_discrepancy_pct,
    safe_divide(g.ga4_sessions, s.shopify_sessions) as ga4_coverage_rate,

    -- Product views
    s.shopify_product_views,
    g.ga4_product_views,
    safe_divide(g.ga4_product_views, s.shopify_product_views) as product_view_coverage_rate,

    -- Add to carts
    s.shopify_add_to_carts,
    g.ga4_add_to_carts,
    safe_divide(g.ga4_add_to_carts, s.shopify_add_to_carts) as add_to_cart_coverage_rate,

    -- Checkouts
    s.shopify_checkouts,
    g.ga4_checkouts,
    safe_divide(g.ga4_checkouts, s.shopify_checkouts) as checkout_coverage_rate,

    -- Orders / Purchases
    s.shopify_orders,
    g.ga4_purchases,
    safe_divide(g.ga4_purchases, s.shopify_orders) as order_coverage_rate,

    -- Funnel conversion rates
    safe_divide(s.shopify_orders, s.shopify_sessions) as shopify_conversion_rate,
    safe_divide(g.ga4_purchases, g.ga4_sessions) as ga4_conversion_rate

from shopify_daily s
full outer join ga4_daily g on s.report_date = g.report_date

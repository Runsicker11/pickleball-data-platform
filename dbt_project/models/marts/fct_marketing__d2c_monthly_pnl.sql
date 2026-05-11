-- D2C monthly contribution P&L roll-up.
-- See int_marketing__d2c_daily_pnl for the underlying line definitions.

{{ config(materialized='table') }}

with monthly as (
    select
        date_trunc(order_date, month) as month,
        sum(orders) as orders,
        sum(units_sold) as units_sold,
        sum(gross_revenue) as gross_revenue,
        sum(net_revenue) as net_revenue,
        sum(total_cogs) as total_cogs,
        sum(estimated_shipping) as estimated_shipping,
        sum(shopify_fees_est) as shopify_fees_est,
        sum(google_ads_spend) as google_ads_spend,
        sum(meta_ads_spend) as meta_ads_spend,
        sum(total_ad_spend) as total_ad_spend,
        sum(d2c_contribution) as d2c_contribution
    from {{ ref('int_marketing__d2c_daily_pnl') }}
    group by month
)

select
    month,
    orders,
    units_sold,
    round(gross_revenue, 2) as gross_revenue,
    round(net_revenue, 2) as net_revenue,
    round(total_cogs, 2) as total_cogs,
    round(estimated_shipping, 2) as estimated_shipping,
    round(shopify_fees_est, 2) as shopify_fees_est,
    round(google_ads_spend, 2) as google_ads_spend,
    round(meta_ads_spend, 2) as meta_ads_spend,
    round(total_ad_spend, 2) as total_ad_spend,
    round(d2c_contribution, 2) as d2c_contribution,
    safe_divide(d2c_contribution, net_revenue) as d2c_contribution_margin,
    safe_divide(total_ad_spend, net_revenue) as ad_spend_pct_of_revenue,
    safe_divide(net_revenue, total_ad_spend) as blended_roas
from monthly
order by month

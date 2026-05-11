-- D2C daily contribution P&L (Shopify direct shop only; Amazon excluded).
--
-- Contribution stack (above-the-line only — no opex allocation):
--   net_revenue
--   - total_cogs            (real from shop_product_cogs + amazon_product_map)
--   - estimated_shipping    ($5.35/order, allocated by line-item revenue share)
--   - shopify_fees_est      (2.9% of net + $0.30 per order; Shopify Payments)
--   - google_ads_spend      (all current Google campaigns are D2C)
--   - meta_ads_spend        (all current Meta campaigns are D2C)
--   = d2c_contribution

with orders as (
    select
        order_date,
        count(distinct order_id) as orders
    from {{ ref('stg_shopify__orders') }}
    group by order_date
),

profitability as (
    select
        order_date,
        sum(units_sold) as units_sold,
        sum(gross_revenue) as gross_revenue,
        sum(net_revenue) as net_revenue,
        sum(total_cogs) as total_cogs,
        sum(estimated_shipping_cost) as estimated_shipping
    from {{ ref('fct_marketing__product_profitability') }}
    group by order_date
),

google_spend as (
    select
        parse_date('%Y-%m-%d', date_start) as order_date,
        sum(spend) as google_ads_spend
    from {{ source('raw_google_ads', 'daily_insights') }}
    where date_start is not null
    group by order_date
),

meta_spend as (
    select
        parse_date('%Y-%m-%d', date_start) as order_date,
        sum(spend) as meta_ads_spend
    from {{ source('raw_meta', 'daily_insights') }}
    where date_start is not null
    group by order_date
),

all_dates as (
    select order_date from orders
    union distinct select order_date from profitability
    union distinct select order_date from google_spend
    union distinct select order_date from meta_spend
),

joined as (
    select
        d.order_date,
        coalesce(o.orders, 0) as orders,
        coalesce(p.units_sold, 0) as units_sold,
        coalesce(p.gross_revenue, 0) as gross_revenue,
        coalesce(p.net_revenue, 0) as net_revenue,
        coalesce(p.total_cogs, 0) as total_cogs,
        coalesce(p.estimated_shipping, 0) as estimated_shipping,
        coalesce(p.net_revenue, 0) * 0.029 + coalesce(o.orders, 0) * 0.30 as shopify_fees_est,
        coalesce(g.google_ads_spend, 0) as google_ads_spend,
        coalesce(m.meta_ads_spend, 0) as meta_ads_spend
    from all_dates d
    left join orders o on d.order_date = o.order_date
    left join profitability p on d.order_date = p.order_date
    left join google_spend g on d.order_date = g.order_date
    left join meta_spend m on d.order_date = m.order_date
)

select
    order_date,
    orders,
    units_sold,
    gross_revenue,
    net_revenue,
    total_cogs,
    estimated_shipping,
    shopify_fees_est,
    google_ads_spend,
    meta_ads_spend,
    google_ads_spend + meta_ads_spend as total_ad_spend,
    net_revenue
        - total_cogs
        - estimated_shipping
        - shopify_fees_est
        - google_ads_spend
        - meta_ads_spend as d2c_contribution,
    safe_divide(
        net_revenue
            - total_cogs
            - estimated_shipping
            - shopify_fees_est
            - google_ads_spend
            - meta_ads_spend,
        net_revenue
    ) as d2c_contribution_margin
from joined
where order_date is not null

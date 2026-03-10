-- 7-day and 30-day rolling averages for spend, revenue, ROAS, orders
-- Replaces: vw_trends

with daily as (
    select
        report_date,
        meta_spend,
        meta_reported_revenue,
        shopify_meta_revenue,
        shopify_meta_orders,
        true_roas
    from {{ ref('int_marketing__true_roas') }}
)

select
    report_date,
    meta_spend,
    shopify_meta_revenue,
    shopify_meta_orders,
    true_roas,

    avg(meta_spend) over (order by report_date rows between 6 preceding and current row) as spend_7d_avg,
    avg(shopify_meta_revenue) over (order by report_date rows between 6 preceding and current row) as revenue_7d_avg,
    avg(true_roas) over (order by report_date rows between 6 preceding and current row) as roas_7d_avg,
    avg(shopify_meta_orders) over (order by report_date rows between 6 preceding and current row) as orders_7d_avg,

    avg(meta_spend) over (order by report_date rows between 29 preceding and current row) as spend_30d_avg,
    avg(shopify_meta_revenue) over (order by report_date rows between 29 preceding and current row) as revenue_30d_avg,
    avg(true_roas) over (order by report_date rows between 29 preceding and current row) as roas_30d_avg,

    meta_spend - lag(meta_spend) over (order by report_date) as spend_dod_change,
    shopify_meta_revenue - lag(shopify_meta_revenue) over (order by report_date) as revenue_dod_change,

    meta_spend - lag(meta_spend, 7) over (order by report_date) as spend_wow_change,
    shopify_meta_revenue - lag(shopify_meta_revenue, 7) over (order by report_date) as revenue_wow_change
from daily

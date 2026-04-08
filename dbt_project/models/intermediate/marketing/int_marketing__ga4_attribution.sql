-- GA4 purchase events matched to Shopify orders via transaction_id
-- Replaces: vw_ga4_attribution

with ga4_purchases_raw as (
    select
        parse_date('%Y%m%d', event_date) as order_date,
        ecommerce.transaction_id,
        coalesce(
            collected_traffic_source.manual_source,
            traffic_source.source
        ) as ga4_source,
        coalesce(
            collected_traffic_source.manual_medium,
            traffic_source.medium
        ) as ga4_medium,
        coalesce(
            collected_traffic_source.manual_campaign_name,
            traffic_source.name
        ) as ga4_campaign,
        cast(null as string) as google_ads_campaign_id,
        ecommerce.purchase_revenue as ga4_revenue,
        user_pseudo_id,
        row_number() over (partition by ecommerce.transaction_id order by event_timestamp desc) as rn
    from {{ source('ga4', 'events_*') }}
    where event_name = 'purchase'
        and ecommerce.transaction_id is not null
        and _table_suffix >= format_date('%Y%m%d', date_sub(current_date(), interval 90 day))
),

ga4_purchases as (
    select * except(rn) from ga4_purchases_raw where rn = 1
)

select
    g.order_date,
    g.transaction_id,
    o.order_id,
    g.ga4_source,
    g.ga4_medium,
    g.ga4_campaign,
    g.google_ads_campaign_id,
    (o.total_price - coalesce(o.total_tax, 0)) as shopify_revenue,
    g.ga4_revenue,
    g.user_pseudo_id
from ga4_purchases g
left join {{ ref('stg_shopify__orders') }} o
    on cast(o.order_id as string) = g.transaction_id

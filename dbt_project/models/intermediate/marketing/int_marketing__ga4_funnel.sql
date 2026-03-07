-- Full purchase funnel from GA4 events by traffic source
-- Replaces: vw_ga4_funnel

with events as (
    select
        parse_date('%Y%m%d', event_date) as report_date,
        event_name,
        user_pseudo_id,
        coalesce(
            collected_traffic_source.manual_source,
            traffic_source.source,
            '(direct)'
        ) as source,
        coalesce(
            collected_traffic_source.manual_medium,
            traffic_source.medium,
            '(none)'
        ) as medium
    from {{ source('ga4', 'events_*') }}
    where event_name in ('session_start', 'page_view', 'view_item', 'add_to_cart', 'begin_checkout', 'purchase')
        and _table_suffix >= format_date('%Y%m%d', date_sub(current_date(), interval 90 day))
)

select
    report_date,
    source,
    medium,
    count(distinct case when event_name = 'session_start' then user_pseudo_id end) as sessions,
    count(distinct case when event_name = 'page_view' then user_pseudo_id end) as page_views,
    count(distinct case when event_name = 'view_item' then user_pseudo_id end) as product_views,
    count(distinct case when event_name = 'add_to_cart' then user_pseudo_id end) as add_to_carts,
    count(distinct case when event_name = 'begin_checkout' then user_pseudo_id end) as checkouts,
    count(distinct case when event_name = 'purchase' then user_pseudo_id end) as purchases,
    safe_divide(
        count(distinct case when event_name = 'add_to_cart' then user_pseudo_id end),
        count(distinct case when event_name = 'view_item' then user_pseudo_id end)
    ) as view_to_cart_rate,
    safe_divide(
        count(distinct case when event_name = 'purchase' then user_pseudo_id end),
        count(distinct case when event_name = 'add_to_cart' then user_pseudo_id end)
    ) as cart_to_purchase_rate,
    safe_divide(
        count(distinct case when event_name = 'purchase' then user_pseudo_id end),
        count(distinct case when event_name = 'session_start' then user_pseudo_id end)
    ) as overall_conversion_rate
from events
group by report_date, source, medium

-- Product-level performance from GA4 events
-- Replaces: vw_ga4_product_insights

with product_events as (
    select
        event_name,
        items.item_name,
        items.item_id,
        items.price,
        items.quantity
    from {{ source('ga4', 'events_*') }},
    unnest(items) as items
    where event_name in ('view_item', 'add_to_cart', 'purchase')
        and items.item_name is not null
        and _table_suffix >= format_date('%Y%m%d', date_sub(current_date(), interval 90 day))
)

select
    item_name,
    item_id,
    countif(event_name = 'view_item') as product_views,
    countif(event_name = 'add_to_cart') as add_to_carts,
    countif(event_name = 'purchase') as purchases,
    sum(case when event_name = 'purchase' then price * quantity else 0 end) as revenue,
    safe_divide(countif(event_name = 'add_to_cart'), countif(event_name = 'view_item')) as view_to_cart_rate,
    safe_divide(countif(event_name = 'purchase'), countif(event_name = 'add_to_cart')) as cart_to_purchase_rate,
    avg(price) as avg_price
from product_events
group by item_name, item_id

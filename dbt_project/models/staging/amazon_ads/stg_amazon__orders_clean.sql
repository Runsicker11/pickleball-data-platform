-- Orders + line items from Amazon SP-API (Seller Central)
-- Replaces the old Airbyte flat-file source

with orders as (
    select * from {{ source('raw_amazon', 'seller_orders') }}
),

items as (
    select * from {{ source('raw_amazon', 'seller_order_items') }}
),

joined as (
    select
        items.seller_sku as sku,
        items.asin,
        coalesce(items.item_price_currency, orders.order_total_currency) as currency,
        cast(coalesce(items.quantity_ordered, 0) as numeric) as quantity,
        cast(coalesce(items.item_price_amount, 0) as numeric) as item_price,
        orders.order_status,
        orders.ship_country,
        orders.sales_channel,
        datetime(cast(orders.purchase_date as timestamp), 'US/Pacific') as purchase_date_pac,
        datetime(cast(orders.purchase_date as timestamp)) as purchase_date,
        orders.amazon_order_id,
        orders.ship_service_level,
        cast(coalesce(items.promotion_discount_amount, 0) as numeric) as item_promotion_discount
    from orders
    inner join items
        on orders.amazon_order_id = items.amazon_order_id
    where orders.order_status != 'Cancelled'
),

conversion as (
    select
        *,
        case
            when currency = 'MXN' then item_price * 0.054
            when currency = 'CAD' then item_price * 0.72
            else item_price
        end as corrected_item_price,
        case
            when currency = 'MXN' then item_promotion_discount * 0.054
            when currency = 'CAD' then item_promotion_discount * 0.72
            else item_promotion_discount
        end as corrected_item_promotion_discount
    from joined
)

select
    *,
    corrected_item_price - corrected_item_promotion_discount as sales
from conversion

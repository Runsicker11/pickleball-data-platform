-- SKU-level profitability with COGS matching
-- Replaces: vw_product_profitability
--
-- COGS resolution order:
--   1. is_partnership (Tuning Clamps)             → 0
--   2. is_gift_card                               → net_revenue (forces contribution to 0)
--   3. shop_product_cogs seed (D2C-specific SKUs) → seed cogs * units
--   4. amazon_product_map                         → map cogs * units
--   5. flat fallback                              → net_revenue * 0.40

with line_items_detail as (
    select
        coalesce(nullif(li.sku, ''), concat('NO_SKU_', replace(li.title, ' ', '_'))) as sku,
        li.title,
        li.order_id,
        date(o.created_at) as order_date,
        li.title = 'Tuning Clamps' as is_partnership,
        lower(li.title) like '%gift card%' as is_gift_card,
        li.quantity,
        cast(li.price as float64) * li.quantity as gross_revenue,
        li.total_discount,
        case when li.title = 'Tuning Clamps'
             then (cast(li.price as float64) * li.quantity - li.total_discount) * 0.40
             else cast(li.price as float64) * li.quantity - li.total_discount
        end as net_revenue
    from {{ ref('stg_shopify__order_line_items') }} li
    join {{ ref('stg_shopify__orders') }} o on li.order_id = o.order_id
),

order_totals as (
    select
        order_id,
        sum(net_revenue) as order_net_revenue
    from line_items_detail
    group by order_id
),

line_items_with_shipping as (
    select
        d.*,
        5.35 * safe_divide(d.net_revenue, ot.order_net_revenue) as item_shipping_cost
    from line_items_detail d
    join order_totals ot on d.order_id = ot.order_id
),

line_items as (
    select
        sku,
        title,
        order_date,
        max(is_partnership) as is_partnership,
        max(is_gift_card) as is_gift_card,
        sum(quantity) as units_sold,
        sum(gross_revenue) as gross_revenue,
        sum(total_discount) as total_discounts,
        sum(net_revenue) as net_revenue,
        sum(item_shipping_cost) as estimated_shipping_cost
    from line_items_with_shipping
    group by sku, title, order_date
),

shop_cogs_lookup as (
    select
        lower(trim(shop_sku)) as sku_key,
        avg(cogs) as unit_cost
    from {{ ref('shop_product_cogs') }}
    group by sku_key
),

amazon_cogs_lookup as (
    select
        lower(trim(sku)) as sku_key,
        avg(cogs) as unit_cost
    from {{ source('product', 'amazon_product_map') }}
    where sku is not null and cogs is not null
    group by sku_key
),

with_cogs as (
    select
        li.*,
        coalesce(shop.unit_cost, amz.unit_cost) as resolved_unit_cost,
        case
            when shop.unit_cost is not null then 'shop_seed'
            when amz.unit_cost is not null then 'amazon_map'
            when li.is_partnership then 'partnership'
            when li.is_gift_card then 'gift_card'
            else 'fallback_40pct'
        end as cogs_source
    from line_items li
    left join shop_cogs_lookup shop on lower(trim(li.sku)) = shop.sku_key
    left join amazon_cogs_lookup amz on lower(trim(li.sku)) = amz.sku_key
),

with_resolved_cogs as (
    select
        *,
        case
            when is_partnership then 0
            when is_gift_card then net_revenue
            when resolved_unit_cost is not null then resolved_unit_cost * units_sold
            else net_revenue * 0.40
        end as total_cogs
    from with_cogs
)

select
    sku,
    title,
    order_date,
    units_sold,
    gross_revenue,
    total_discounts,
    net_revenue,
    is_partnership,
    is_gift_card,
    cogs_source,
    total_cogs,
    net_revenue - total_cogs as gross_profit,
    safe_divide(net_revenue - total_cogs, net_revenue) as gross_margin,
    cogs_source in ('shop_seed', 'amazon_map', 'partnership', 'gift_card') as has_actual_cogs,
    estimated_shipping_cost,
    net_revenue - total_cogs - estimated_shipping_cost as contribution_profit,
    safe_divide(net_revenue - total_cogs - estimated_shipping_cost, net_revenue) as contribution_margin
from with_resolved_cogs

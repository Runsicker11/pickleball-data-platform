-- SKU-level profitability with COGS matching
-- Replaces: vw_product_profitability

with line_items_detail as (
    select
        li.sku,
        li.title,
        li.order_id,
        date(o.created_at) as order_date,
        li.title = 'Tuning Clamps' as is_partnership,
        li.quantity,
        cast(li.price as float64) * li.quantity as gross_revenue,
        li.total_discount,
        case when li.title = 'Tuning Clamps'
             then (cast(li.price as float64) * li.quantity - li.total_discount) * 0.40
             else cast(li.price as float64) * li.quantity - li.total_discount
        end as net_revenue
    from {{ ref('stg_shopify__order_line_items') }} li
    join {{ ref('stg_shopify__orders') }} o on li.order_id = o.order_id
    where li.sku is not null and li.sku != ''
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
        sum(quantity) as units_sold,
        sum(gross_revenue) as gross_revenue,
        sum(total_discount) as total_discounts,
        sum(net_revenue) as net_revenue,
        sum(item_shipping_cost) as estimated_shipping_cost
    from line_items_with_shipping
    group by sku, title, order_date
),

cogs_lookup as (
    select
        lower(trim(sku)) as sku_key,
        avg(cogs) as unit_cost
    from {{ source('product', 'amazon_product_map') }}
    where sku is not null and cogs is not null
    group by sku_key
)

select
    li.sku,
    li.title,
    li.order_date,
    li.units_sold,
    li.gross_revenue,
    li.total_discounts,
    li.net_revenue,
    li.is_partnership,
    case
        when li.is_partnership then 0
        when c.unit_cost is not null then c.unit_cost * li.units_sold
        else li.net_revenue * 0.40
    end as total_cogs,
    li.net_revenue - case
        when li.is_partnership then 0
        when c.unit_cost is not null then c.unit_cost * li.units_sold
        else li.net_revenue * 0.40
    end as gross_profit,
    safe_divide(
        li.net_revenue - case
            when li.is_partnership then 0
            when c.unit_cost is not null then c.unit_cost * li.units_sold
            else li.net_revenue * 0.40
        end,
        li.net_revenue
    ) as gross_margin,
    case
        when li.is_partnership then true
        when c.unit_cost is not null then true
        else false
    end as has_actual_cogs,
    li.estimated_shipping_cost,
    li.net_revenue - case
        when li.is_partnership then 0
        when c.unit_cost is not null then c.unit_cost * li.units_sold
        else li.net_revenue * 0.40
    end - li.estimated_shipping_cost as contribution_profit,
    safe_divide(
        li.net_revenue - case
            when li.is_partnership then 0
            when c.unit_cost is not null then c.unit_cost * li.units_sold
            else li.net_revenue * 0.40
        end - li.estimated_shipping_cost,
        li.net_revenue
    ) as contribution_margin
from line_items li
left join cogs_lookup c on lower(trim(li.sku)) = c.sku_key

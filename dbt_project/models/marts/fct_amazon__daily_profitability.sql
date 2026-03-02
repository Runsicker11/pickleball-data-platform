{{
  config(
    materialized='table'
  )
}}

with daily_orders as (
    select * from {{ ref('int_amazon__daily_orders') }}
),

daily_ad_spend as (
    select * from {{ ref('int_amazon__daily_ad_spend') }}
),

product_map as (
    select * from {{ source('product', 'amazon_product_map') }}
),

joined as (
    select
        daily_orders.purchase_date_utc,
        daily_orders.asin,
        daily_orders.quantity,
        daily_orders.item_price,
        daily_orders.item_promotion_discount,
        daily_ad_spend.impressions,
        daily_ad_spend.clicks,
        daily_ad_spend.ad_sales,
        daily_orders.sales,
        coalesce(daily_ad_spend.cost, 0) as ad_cost,
        (product_map.cogs * daily_orders.quantity) as total_cogs,
        product_map.cogs,
        product_map.fba_cost,
        (product_map.fba_cost * daily_orders.quantity) as total_fba_cost,
        daily_orders.sales * 0.15 as selling_fees,
        daily_orders.sales * 0.03 as refunds,
        daily_orders.sales * 0.01 as shipping_fees,
        product_map.title,
        product_map.category,
        product_map.subcategory,
        product_map.sku
    from daily_orders
    left join daily_ad_spend
        on daily_orders.purchase_date_utc = cast(daily_ad_spend.date as date)
        and daily_orders.asin = daily_ad_spend.asin
    left join product_map
        on daily_orders.asin = product_map.asin
),

final as (
    select
        *,
        total_cogs + total_fba_cost + ad_cost + selling_fees + refunds + shipping_fees as total_costs,
        sales - (total_cogs + total_fba_cost + ad_cost + selling_fees + refunds + shipping_fees) as contribution
    from joined
)

select * from final

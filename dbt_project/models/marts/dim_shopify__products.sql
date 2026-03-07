{{
  config(
    materialized='table'
  )
}}

-- Product dimension table joining products with variants
with products as (
    select * from {{ ref('stg_shopify__products') }}
),

variants as (
    select
        product_id,
        count(*) as variant_count,
        min(price) as min_price,
        max(price) as max_price,
        sum(inventory_quantity) as total_inventory,
        string_agg(distinct sku, ', ' order by sku) as skus
    from {{ ref('stg_shopify__product_variants') }}
    where sku is not null and sku != ''
    group by product_id
)

select
    p.product_id,
    p.title,
    p.handle,
    p.product_type,
    p.vendor,
    p.status,
    p.tags,
    p.created_at,
    p.updated_at,
    v.variant_count,
    v.min_price,
    v.max_price,
    v.total_inventory,
    v.skus
from products p
left join variants v on p.product_id = v.product_id

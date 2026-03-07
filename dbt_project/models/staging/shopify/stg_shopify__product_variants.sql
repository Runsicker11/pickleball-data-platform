with source as (
    select * from {{ source('raw_shopify', 'product_variants') }}
),

renamed as (
    select
        cast(variant_id as int64) as variant_id,
        cast(product_id as int64) as product_id,
        cast(price as float64) as price,
        cast(compare_at_price as float64) as compare_at_price,
        cast(inventory_quantity as int64) as inventory_quantity,
        title,
        sku

    from source
)

select * from renamed

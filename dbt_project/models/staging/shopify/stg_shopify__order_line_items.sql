with source as (
    select * from {{ source('raw_shopify', 'order_line_items') }}
),

renamed as (
    select
        cast(line_item_id as int64) as line_item_id,
        cast(order_id as int64) as order_id,
        cast(product_id as int64) as product_id,
        cast(variant_id as int64) as variant_id,
        cast(quantity as int64) as quantity,
        cast(price as float64) as price,
        cast(total_discount as float64) as total_discount,
        cast(order_date as date) as order_date,
        title,
        variant_title,
        sku

    from source
)

select * from renamed

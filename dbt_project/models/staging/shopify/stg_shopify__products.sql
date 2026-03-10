with source as (
    select * from {{ source('raw_shopify', 'products') }}
),

renamed as (
    select
        cast(product_id as int64) as product_id,
        title,
        handle,
        product_type,
        vendor,
        status,
        tags,
        created_at,
        updated_at

    from source
)

select * from renamed

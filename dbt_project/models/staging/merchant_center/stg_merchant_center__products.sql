with source as (
    select * from {{ source('raw_merchant_center', 'products') }}
),

renamed as (
    select
        product_id,
        title,
        description,
        link,
        image_link,
        availability,
        price_value,
        price_currency,
        brand,
        condition,
        custom_label_0,
        custom_label_1,
        custom_label_2,
        custom_label_3,
        custom_label_4,
        product_types,
        google_product_category,
        cast(ingested_at as timestamp) as ingested_at

    from source
)

select * from renamed

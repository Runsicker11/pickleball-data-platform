with source as (
    select * from {{ source('raw_google_ads', 'shopping_performance') }}
),

renamed as (
    select
        cast(date_start as date) as date_start,
        product_title,
        product_item_id,
        product_type_l1,
        product_brand,
        cast(campaign_id as int64) as campaign_id,
        campaign_name,
        cast(impressions as int64) as impressions,
        cast(clicks as int64) as clicks,
        cast(spend as float64) as spend,
        cast(conversions as float64) as conversions,
        cast(conversion_value as float64) as conversion_value

    from source
)

select * from renamed

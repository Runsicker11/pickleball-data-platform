with source as (
    select * from {{ source('raw_shopify', 'shopify_sessions') }}
),

renamed as (
    select
        cast(report_date as date) as report_date,
        cast(referrer_source as string) as referrer_source,
        cast(sessions as int64) as sessions,
        cast(product_views as int64) as product_views,
        cast(add_to_carts as int64) as add_to_carts,
        cast(checkouts as int64) as checkouts,
        cast(orders as int64) as orders

    from source
)

select * from renamed

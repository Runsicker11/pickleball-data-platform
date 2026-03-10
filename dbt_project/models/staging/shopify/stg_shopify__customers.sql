with source as (
    select * from {{ source('raw_shopify', 'customers') }}
),

renamed as (
    select
        cast(customer_id as int64) as customer_id,
        cast(orders_count as int64) as orders_count,
        cast(total_spent as float64) as total_spent,
        email,
        first_name,
        last_name,
        state,
        accepts_marketing,
        city,
        province,
        country,
        tags

    from source
)

select * from renamed

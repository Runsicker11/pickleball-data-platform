with source as (
    select * from {{ source('raw_shopify', 'orders') }}
),

renamed as (
    select
        cast(order_id as int64) as order_id,
        cast(order_number as int64) as order_number,
        cast(order_date as date) as order_date,
        cast(total_price as float64) as total_price,
        cast(subtotal_price as float64) as subtotal_price,
        cast(total_tax as float64) as total_tax,
        cast(total_shipping as float64) as total_shipping,
        cast(total_discounts as float64) as total_discounts,
        cast(customer_id as int64) as customer_id,
        customer_email,
        financial_status,
        fulfillment_status,
        currency,
        source_name,
        referring_site,
        landing_site,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        cancelled_at,
        cancel_reason,
        note,
        tags,
        created_at,
        updated_at

    from source
)

select * from renamed

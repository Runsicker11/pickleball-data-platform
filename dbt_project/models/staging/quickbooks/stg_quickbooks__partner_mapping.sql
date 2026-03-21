with source as (
    select * from {{ source('raw_quickbooks', 'partner_mapping') }}
),

renamed as (
    select
        sender_name,
        sender_email,
        partner_brand,
        cast(payments as int64) as payment_count,
        cast(total_amount as float64) as total_amount,
        currencies,
        first_payment,
        last_payment,
        sample_subjects
    from source
    where sender_name is not null and sender_name != ''
)

select * from renamed

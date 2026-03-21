with source as (
    select * from {{ source('raw_quickbooks', 'payments') }}
),

renamed as (
    select
        cast(payment_id as string) as payment_id,
        cast(txn_date as date) as txn_date,
        cast(customer_id as string) as customer_id,
        customer_name,
        cast(total_amount as float64) as total_amount,
        cast(unapplied_amount as float64) as unapplied_amount,
        cast(payment_method_id as string) as payment_method_id,
        cast(deposit_to_account_id as string) as deposit_to_account_id,
        currency,
        created_at,
        updated_at

    from source
)

select * from renamed

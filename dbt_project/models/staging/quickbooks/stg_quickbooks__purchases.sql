with source as (
    select * from {{ source('raw_quickbooks', 'purchases') }}
),

renamed as (
    select
        cast(purchase_id as string) as purchase_id,
        cast(txn_date as date) as txn_date,
        payment_type,
        cast(total_amount as float64) as total_amount,
        cast(account_id as string) as account_id,
        account_name,
        cast(vendor_id as string) as vendor_id,
        vendor_name,
        memo,
        currency,
        cast(credit as bool) as credit,
        cast(line_count as int64) as line_count,
        line_summary,
        created_at,
        updated_at

    from source
)

select * from renamed

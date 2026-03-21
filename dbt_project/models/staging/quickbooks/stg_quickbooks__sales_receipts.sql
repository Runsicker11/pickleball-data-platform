with source as (
    select * from {{ source('raw_quickbooks', 'sales_receipts') }}
),

renamed as (
    select
        cast(sales_receipt_id as string) as sales_receipt_id,
        cast(txn_date as date) as txn_date,
        cast(customer_id as string) as customer_id,
        customer_name,
        cast(total_amount as float64) as total_amount,
        cast(subtotal_amount as float64) as subtotal_amount,
        payment_method_name,
        deposit_to_account_name,
        currency,
        memo,
        created_at,
        updated_at

    from source
)

select * from renamed

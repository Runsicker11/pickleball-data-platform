with source as (
    select * from {{ source('raw_quickbooks', 'purchase_line_items') }}
),

renamed as (
    select
        cast(line_item_id as string) as line_item_id,
        cast(purchase_id as string) as purchase_id,
        cast(txn_date as date) as txn_date,
        cast(vendor_id as string) as vendor_id,
        vendor_name,
        description,
        cast(amount as float64) as amount,
        cast(account_id as string) as account_id,
        account_name,
        cast(customer_id as string) as customer_id,
        customer_name,
        billable_status

    from source
)

select * from renamed

with source as (
    select * from {{ source('raw_quickbooks', 'invoices') }}
),

renamed as (
    select
        cast(invoice_id as string) as invoice_id,
        doc_number,
        cast(txn_date as date) as txn_date,
        cast(due_date as date) as due_date,
        cast(customer_id as string) as customer_id,
        customer_name,
        cast(total_amount as float64) as total_amount,
        cast(balance as float64) as balance,
        currency,
        email_status,
        billing_email,
        cast(ship_date as date) as ship_date,
        tracking_num,
        txn_status,
        created_at,
        updated_at

    from source
)

select * from renamed

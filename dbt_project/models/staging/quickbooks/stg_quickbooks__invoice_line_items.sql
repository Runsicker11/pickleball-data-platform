with source as (
    select * from {{ source('raw_quickbooks', 'invoice_line_items') }}
),

renamed as (
    select
        cast(line_item_id as string) as line_item_id,
        cast(invoice_id as string) as invoice_id,
        cast(txn_date as date) as txn_date,
        cast(customer_id as string) as customer_id,
        cast(line_num as int64) as line_num,
        description,
        cast(item_id as string) as item_id,
        item_name,
        cast(quantity as float64) as quantity,
        cast(unit_price as float64) as unit_price,
        cast(amount as float64) as amount,
        cast(discount_rate as float64) as discount_rate,
        tax_code

    from source
)

select * from renamed

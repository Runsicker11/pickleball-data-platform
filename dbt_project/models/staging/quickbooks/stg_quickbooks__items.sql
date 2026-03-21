with source as (
    select * from {{ source('raw_quickbooks', 'items') }}
),

renamed as (
    select
        cast(item_id as string) as item_id,
        name,
        fully_qualified_name,
        type,
        description,
        cast(unit_price as float64) as unit_price,
        cast(purchase_cost as float64) as purchase_cost,
        sku,
        income_account_id,
        income_account_name,
        cast(active as bool) as active,
        cast(taxable as bool) as taxable,
        created_at,
        updated_at

    from source
)

select * from renamed

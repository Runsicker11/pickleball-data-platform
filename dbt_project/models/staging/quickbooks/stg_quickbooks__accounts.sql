with source as (
    select * from {{ source('raw_quickbooks', 'accounts') }}
),

renamed as (
    select
        cast(account_id as string) as account_id,
        name,
        fully_qualified_name,
        account_type,
        account_sub_type,
        classification,
        cast(current_balance as float64) as current_balance,
        currency,
        cast(active as bool) as active,
        created_at,
        updated_at

    from source
)

select * from renamed

with source as (
    select * from {{ source('raw_quickbooks', 'vendors') }}
),

renamed as (
    select
        cast(vendor_id as string) as vendor_id,
        display_name,
        company_name,
        given_name,
        family_name,
        email,
        cast(balance as float64) as balance,
        cast(active as bool) as active,
        cast(vendor_1099 as bool) as vendor_1099,
        created_at,
        updated_at

    from source
)

select * from renamed

with source as (
    select * from {{ source('raw_quickbooks', 'customers') }}
),

renamed as (
    select
        cast(customer_id as string) as customer_id,
        display_name,
        company_name,
        given_name,
        family_name,
        email,
        phone,
        city,
        state,
        postal_code,
        country,
        cast(balance as float64) as balance,
        cast(active as bool) as active,
        cast(is_project as bool) as is_project,
        created_at,
        updated_at

    from source
)

select * from renamed

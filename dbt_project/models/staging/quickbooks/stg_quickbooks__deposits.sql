with source as (
    select * from {{ source('raw_quickbooks', 'deposits') }}
),

renamed as (
    select
        cast(deposit_id as string) as deposit_id,
        cast(txn_date as date) as txn_date,
        cast(total_amount as float64) as total_amount,
        deposit_to_account_name,
        source_summary,
        memo,
        cast(line_count as int64) as line_count,
        currency,
        created_at,
        updated_at

    from source
)

select * from renamed

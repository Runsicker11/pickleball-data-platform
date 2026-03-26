with source as (
    select * from {{ source('raw_google_trends', 'interest_over_time') }}
),

renamed as (
    select
        cast(week as date) as week,
        keyword,
        cast(interest_score as int64) as interest_score,
        cast(is_partial as boolean) as is_partial,
        cast(ingested_at as timestamp) as ingested_at

    from source
)

select * from renamed

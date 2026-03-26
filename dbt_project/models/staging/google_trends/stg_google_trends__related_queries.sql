with source as (
    select * from {{ source('raw_google_trends', 'related_queries') }}
),

renamed as (
    select
        keyword,
        query_type,
        related_query,
        cast(value as int64) as value,
        cast(ingested_at as timestamp) as ingested_at

    from source
)

select * from renamed

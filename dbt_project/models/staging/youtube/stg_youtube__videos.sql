with source as (
    select * from {{ source('raw_youtube', 'videos') }}
),

renamed as (
    select
        video_id,
        title,
        description,
        timestamp(published_at) as published_at,
        date(timestamp(published_at)) as published_date,
        channel_id,
        tags,
        category_id,
        duration,                   -- ISO 8601 e.g. PT12M3S
        thumbnail_url,
        privacy_status,
        made_for_kids,
        cast(ingested_at as timestamp) as ingested_at
    from source
)

select * from renamed

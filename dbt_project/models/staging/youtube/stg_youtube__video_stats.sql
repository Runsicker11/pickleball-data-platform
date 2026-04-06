with source as (
    select * from {{ source('raw_youtube', 'video_stats') }}
),

renamed as (
    select
        video_id,
        cast(snapshot_date as date) as snapshot_date,
        view_count,
        like_count,
        comment_count,
        favorite_count,
        cast(ingested_at as timestamp) as ingested_at
    from source
)

select * from renamed

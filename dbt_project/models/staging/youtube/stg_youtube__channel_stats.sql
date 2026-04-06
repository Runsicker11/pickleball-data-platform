with source as (
    select * from {{ source('raw_youtube', 'channel_stats') }}
),

renamed as (
    select
        channel_id,
        title as channel_title,
        cast(snapshot_date as date) as snapshot_date,
        subscriber_count,
        view_count,
        video_count,
        hidden_subscriber_count,
        cast(ingested_at as timestamp) as ingested_at
    from source
)

select * from renamed

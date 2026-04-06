with source as (
    select * from {{ source('raw_youtube', 'video_analytics') }}
),

renamed as (
    select
        video_id,
        cast(report_date as date) as report_date,
        views,
        estimated_minutes_watched,
        round(avg_view_duration_seconds / 60.0, 2) as avg_view_duration_minutes,
        avg_view_duration_seconds,
        round(avg_view_percentage, 4) as avg_view_percentage,
        likes,
        comments,
        shares,
        subscribers_gained,
        subscribers_lost,
        subscribers_gained - subscribers_lost as net_subscribers,
        cast(ingested_at as timestamp) as ingested_at
    from source
)

select * from renamed

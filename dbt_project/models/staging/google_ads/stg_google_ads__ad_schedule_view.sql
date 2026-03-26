with source as (
    select * from {{ source('raw_google_ads', 'ad_schedule_view') }}
),

renamed as (
    select
        cast(date_start as date) as date_start,
        cast(campaign_id as int64) as campaign_id,
        campaign_name,
        cast(criterion_id as int64) as criterion_id,
        day_of_week,
        cast(start_hour as int64) as start_hour,
        cast(end_hour as int64) as end_hour,
        start_minute,
        end_minute,
        cast(impressions as int64) as impressions,
        cast(clicks as int64) as clicks,
        cast(cost as float64) as cost,
        cast(conversions as float64) as conversions,
        cast(conversions_value as float64) as conversions_value,
        cast(ingested_at as timestamp) as ingested_at
    from source
)

select * from renamed

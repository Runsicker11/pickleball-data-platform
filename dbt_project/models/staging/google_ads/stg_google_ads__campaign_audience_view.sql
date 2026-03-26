with source as (
    select * from {{ source('raw_google_ads', 'campaign_audience_view') }}
),

renamed as (
    select
        resource_name,
        cast(campaign_id as int64) as campaign_id,
        campaign_name,
        cast(impressions as int64) as impressions,
        cast(clicks as int64) as clicks,
        cast(spend as float64) as spend,
        cast(conversions as float64) as conversions,
        cast(conversion_value as float64) as conversion_value

    from source
)

select * from renamed

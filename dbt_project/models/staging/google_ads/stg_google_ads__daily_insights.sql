with source as (
    select * from {{ source('raw_google_ads', 'daily_insights') }}
),

renamed as (
    select
        cast(date_start as date) as date_start,
        cast(campaign_id as int64) as campaign_id,
        campaign_name,
        campaign_type,
        cast(ad_group_id as int64) as ad_group_id,
        ad_group_name,
        cast(impressions as int64) as impressions,
        cast(clicks as int64) as clicks,
        cast(spend as float64) as spend,
        cast(cpc as float64) as cpc,
        cast(ctr as float64) as ctr,
        cast(conversions as float64) as conversions,
        cast(conversion_value as float64) as conversion_value,
        cast(cost_per_conversion as float64) as cost_per_conversion,
        cast(search_impression_share as float64) as search_impression_share

    from source
)

select * from renamed

with source as (
    select * from {{ source('raw_google_ads', 'pmax_insights') }}
),

renamed as (
    select
        date_start,
        cast(campaign_id as int64) as campaign_id,
        campaign_name,
        impressions,
        clicks,
        spend,
        conversions,
        conversion_value,
        SAFE_DIVIDE(conversion_value, NULLIF(spend, 0)) as roas,
        SAFE_DIVIDE(spend, NULLIF(conversions, 0)) as cpa

    from source
)

select * from renamed

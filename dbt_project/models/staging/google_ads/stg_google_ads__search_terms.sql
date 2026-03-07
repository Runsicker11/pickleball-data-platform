with source as (
    select * from {{ source('raw_google_ads', 'search_terms') }}
),

renamed as (
    select
        cast(date_start as date) as date_start,
        cast(campaign_id as int64) as campaign_id,
        campaign_name,
        cast(ad_group_id as int64) as ad_group_id,
        ad_group_name,
        search_term,
        keyword_text,
        match_type,
        cast(impressions as int64) as impressions,
        cast(clicks as int64) as clicks,
        cast(spend as float64) as spend,
        cast(conversions as float64) as conversions,
        cast(conversion_value as float64) as conversion_value

    from source
)

select * from renamed

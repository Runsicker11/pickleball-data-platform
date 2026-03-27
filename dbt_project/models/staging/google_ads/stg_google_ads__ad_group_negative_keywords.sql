with source as (
    select * from {{ source('raw_google_ads', 'ad_group_negative_keywords') }}
),

renamed as (
    select
        cast(criterion_id as int64) as criterion_id,
        cast(ad_group_id as int64) as ad_group_id,
        ad_group_name,
        cast(campaign_id as int64) as campaign_id,
        campaign_name,
        keyword_text,
        match_type,
        status

    from source
)

select * from renamed

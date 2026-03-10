with source as (
    select * from {{ source('raw_google_ads', 'keywords') }}
),

renamed as (
    select
        cast(keyword_id as int64) as keyword_id,
        cast(ad_group_id as int64) as ad_group_id,
        cast(campaign_id as int64) as campaign_id,
        keyword_text,
        match_type,
        status,
        cast(quality_score as int64) as quality_score,
        expected_ctr,
        ad_relevance,
        landing_page_experience

    from source
)

select * from renamed

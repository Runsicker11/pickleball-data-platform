with source as (
    select * from {{ source('raw_google_ads', 'campaign_negative_keywords') }}
),

renamed as (
    select
        cast(criterion_id as int64) as criterion_id,
        cast(campaign_id as int64) as campaign_id,
        campaign_name,
        keyword_text,
        match_type,
        status

    from source
)

select * from renamed

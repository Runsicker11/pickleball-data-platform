with source as (
    select * from {{ source('raw_meta', 'ads') }}
),

renamed as (
    select
        cast(ad_id as string) as ad_id,
        cast(adset_id as string) as adset_id,
        cast(campaign_id as string) as campaign_id,
        cast(creative_id as string) as creative_id,
        ad_name,
        status

    from source
)

select * from renamed

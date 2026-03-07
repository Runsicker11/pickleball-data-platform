with source as (
    select * from {{ source('raw_google_ads', 'ad_groups') }}
),

renamed as (
    select
        cast(ad_group_id as int64) as ad_group_id,
        cast(campaign_id as int64) as campaign_id,
        ad_group_name,
        ad_group_type,
        status,
        cast(cpc_bid_micros as float64) as cpc_bid_micros

    from source
)

select * from renamed

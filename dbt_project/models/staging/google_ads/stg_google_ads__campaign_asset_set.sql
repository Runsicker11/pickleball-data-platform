with source as (
    select * from {{ source('raw_google_ads', 'campaign_asset_set') }}
),

renamed as (
    select
        campaign_resource_name,
        asset_set_resource_name,
        status,
        cast(asset_set_id as int64) as asset_set_id,
        asset_set_name,
        asset_set_type,
        cast(campaign_id as int64) as campaign_id,
        campaign_name

    from source
)

select * from renamed

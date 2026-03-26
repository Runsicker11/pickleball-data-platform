with source as (
    select * from {{ source('raw_google_ads', 'asset_group_asset') }}
),

renamed as (
    select
        asset_resource_name,
        field_type,
        status,
        cast(asset_group_id as int64) as asset_group_id,
        asset_group_name,
        cast(campaign_id as int64) as campaign_id,
        campaign_name

    from source
)

select * from renamed

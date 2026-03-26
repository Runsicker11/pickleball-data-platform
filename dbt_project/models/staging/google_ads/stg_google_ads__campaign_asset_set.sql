{% set relation = adapter.get_relation(
    database='practical-gecko-373320',
    schema='raw_google_ads',
    identifier='campaign_asset_set'
) %}

{% if relation is not none %}
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

{% else %}

-- source table not yet populated (no campaign asset sets / shopping feed links found)
select
    cast(null as string) as campaign_resource_name,
    cast(null as string) as asset_set_resource_name,
    cast(null as string) as status,
    cast(null as int64) as asset_set_id,
    cast(null as string) as asset_set_name,
    cast(null as string) as asset_set_type,
    cast(null as int64) as campaign_id,
    cast(null as string) as campaign_name
from (select 1) where false

{% endif %}

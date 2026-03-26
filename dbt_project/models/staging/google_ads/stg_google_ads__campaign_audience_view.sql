{% set relation = adapter.get_relation(
    database='practical-gecko-373320',
    schema='raw_google_ads',
    identifier='campaign_audience_view'
) %}

{% if relation is not none %}
with source as (
    select * from {{ source('raw_google_ads', 'campaign_audience_view') }}
),

renamed as (
    select
        resource_name,
        cast(campaign_id as int64) as campaign_id,
        campaign_name,
        cast(impressions as int64) as impressions,
        cast(clicks as int64) as clicks,
        cast(spend as float64) as spend,
        cast(conversions as float64) as conversions,
        cast(conversion_value as float64) as conversion_value

    from source
)

select * from renamed

{% else %}

-- source table not yet populated (no campaign-level audience observations found)
select
    cast(null as string) as resource_name,
    cast(null as int64) as campaign_id,
    cast(null as string) as campaign_name,
    cast(null as int64) as impressions,
    cast(null as int64) as clicks,
    cast(null as float64) as spend,
    cast(null as float64) as conversions,
    cast(null as float64) as conversion_value
from (select 1) where false

{% endif %}

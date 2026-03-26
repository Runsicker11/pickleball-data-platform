with source as (
    select * from {{ source('raw_klaviyo', 'campaign_metrics') }}
),

renamed as (
    select
        campaign_id,
        campaign_name,
        cast(send_date as date) as send_date,
        -- Metric columns are populated once Klaviyo per-campaign API calls succeed.
        -- They may be absent on initial load; cast NULL to preserve schema until then.
        {% set src_cols = adapter.get_columns_in_relation(source('raw_klaviyo', 'campaign_metrics')) | map(attribute='name') | list %}
        cast({% if 'recipients' in src_cols %}recipients{% else %}null{% endif %} as int64) as recipients,
        cast({% if 'opens' in src_cols %}opens{% else %}null{% endif %} as float64) as opens,
        cast({% if 'clicks' in src_cols %}clicks{% else %}null{% endif %} as float64) as clicks,
        cast({% if 'revenue' in src_cols %}revenue{% else %}null{% endif %} as float64) as revenue,
        cast({% if 'unsubscribes' in src_cols %}unsubscribes{% else %}null{% endif %} as float64) as unsubscribes,
        cast(ingested_at as timestamp) as ingested_at
    from source
)

select * from renamed

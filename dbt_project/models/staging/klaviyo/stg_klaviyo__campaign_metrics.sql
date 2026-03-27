with source as (
    select * from {{ source('raw_klaviyo', 'campaign_metrics') }}
),

renamed as (
    select
        campaign_id,
        campaign_name,
        cast(send_date as date) as send_date,
        cast(recipients as float64) as recipients,
        cast(delivered as float64) as delivered,
        cast(opens as float64) as opens,
        cast(clicks as float64) as clicks,
        cast(unsubscribes as float64) as unsubscribes,
        cast(conversions as float64) as conversions,
        cast(revenue as float64) as revenue,
        cast(ingested_at as timestamp) as ingested_at
    from source
)

select * from renamed

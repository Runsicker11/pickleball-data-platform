with source as (
    select * from {{ source('raw_klaviyo', 'metrics_timeline') }}
),

renamed as (
    select
        cast(date as date) as date,
        metric_id,
        metric_name,
        cast(value as float64) as value,
        cast(ingested_at as timestamp) as ingested_at
    from source
)

select * from renamed

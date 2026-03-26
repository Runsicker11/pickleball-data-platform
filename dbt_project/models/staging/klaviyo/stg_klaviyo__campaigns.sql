with source as (
    select * from {{ source('raw_klaviyo', 'campaigns') }}
),

renamed as (
    select
        id as campaign_id,
        name as campaign_name,
        status,
        cast(send_time as timestamp) as send_time,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(ingested_at as timestamp) as ingested_at
    from source
)

select * from renamed

with source as (
    select * from {{ source('raw_klaviyo', 'flows') }}
),

renamed as (
    select
        id as flow_id,
        name as flow_name,
        status,
        trigger_type,
        cast(created as timestamp) as created_at,
        cast(updated as timestamp) as updated_at,
        cast(ingested_at as timestamp) as ingested_at
    from source
)

select * from renamed

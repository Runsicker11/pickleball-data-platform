with source as (
    select * from {{ source('raw_google_ads', 'conversion_action') }}
),

renamed as (
    select
        cast(conversion_action_id as int64) as conversion_action_id,
        conversion_action_name,
        status,
        conversion_type,
        cast(default_value as float64) as default_value,
        cast(always_use_default_value as bool) as always_use_default_value,
        counting_type,
        cast(include_in_conversions_metric as bool) as include_in_conversions_metric

    from source
)

select * from renamed

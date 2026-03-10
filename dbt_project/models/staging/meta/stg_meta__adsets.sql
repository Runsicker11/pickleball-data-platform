with source as (
    select * from {{ source('raw_meta', 'adsets') }}
),

renamed as (
    select
        cast(adset_id as string) as adset_id,
        cast(campaign_id as string) as campaign_id,
        adset_name,
        status,
        targeting_summary,
        optimization_goal,
        billing_event

    from source
)

select * from renamed

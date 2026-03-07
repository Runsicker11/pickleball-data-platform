with source as (
    select * from {{ source('raw_meta', 'campaigns') }}
),

renamed as (
    select
        cast(campaign_id as string) as campaign_id,
        campaign_name,
        objective,
        status,
        cast(daily_budget as float64) as daily_budget,
        created_time,
        updated_time

    from source
)

select * from renamed

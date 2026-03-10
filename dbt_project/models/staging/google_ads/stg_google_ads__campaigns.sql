with source as (
    select * from {{ source('raw_google_ads', 'campaigns') }}
),

renamed as (
    select
        cast(campaign_id as int64) as campaign_id,
        campaign_name,
        campaign_type,
        bidding_strategy_type,
        status,
        cast(budget_amount as float64) as budget_amount

    from source
)

select * from renamed

with source as (
    select * from {{ source('raw_google_ads', 'bidding_strategy') }}
),

renamed as (
    select
        cast(bidding_strategy_id as int64) as bidding_strategy_id,
        bidding_strategy_name,
        bidding_strategy_type,
        safe_divide(cast(target_cpa_micros as float64), 1000000) as target_cpa,
        cast(target_roas as float64) as target_roas,
        safe_divide(cast(maximize_conversions_target_cpa_micros as float64), 1000000) as maximize_conversions_target_cpa

    from source
)

select * from renamed

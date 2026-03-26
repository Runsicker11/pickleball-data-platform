{% set relation = adapter.get_relation(
    database='practical-gecko-373320',
    schema='raw_google_ads',
    identifier='bidding_strategy'
) %}

{% if relation is not none %}
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

{% else %}

-- source table not yet populated (no portfolio bidding strategies configured)
select
    cast(null as int64) as bidding_strategy_id,
    cast(null as string) as bidding_strategy_name,
    cast(null as string) as bidding_strategy_type,
    cast(null as float64) as target_cpa,
    cast(null as float64) as target_roas,
    cast(null as float64) as maximize_conversions_target_cpa
from (select 1) where false

{% endif %}

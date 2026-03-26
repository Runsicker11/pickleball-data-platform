with source as (
    select * from {{ source('raw_google_ads', 'auction_insights') }}
),

renamed as (
    select
        cast(date as date) as date,
        cast(campaign_id as int64) as campaign_id,
        campaign_name,
        competitor_domain,
        cast(impression_share as float64) as impression_share,
        cast(overlap_rate as float64) as overlap_rate,
        cast(position_above_rate as float64) as position_above_rate,
        cast(top_of_page_rate as float64) as top_of_page_rate,
        cast(outranking_share as float64) as outranking_share,
        cast(ingested_at as timestamp) as ingested_at

    from source
)

select * from renamed

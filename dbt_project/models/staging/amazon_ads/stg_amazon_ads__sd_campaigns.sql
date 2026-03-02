with source as (
    select * from {{ source('raw_amazon', 'sd_campaigns') }}
),

renamed as (
    select
        cast(date as date) as report_date,
        cast(campaign_id as string) as campaign_id,
        campaign_name,
        campaign_status,
        cast(profile_id as string) as profile_id,
        'SPONSORED_DISPLAY' as ad_product,

        -- Metrics
        cast(impressions as int64) as impressions,
        cast(clicks as int64) as clicks,
        cast(cost as float64) as cost,
        cast(purchases14d as int64) as purchases,
        cast(sales14d as float64) as sales,
        cast(units_sold_clicks14d as int64) as units_sold,

        -- Calculated metrics
        safe_divide(cast(clicks as float64), cast(impressions as float64)) as ctr,
        safe_divide(cast(cost as float64), cast(clicks as float64)) as cpc,
        safe_divide(cast(cost as float64), cast(sales14d as float64)) as acos,
        safe_divide(cast(sales14d as float64), cast(cost as float64)) as roas,

        _loaded_at

    from source
)

select * from renamed

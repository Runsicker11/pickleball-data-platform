-- Ad spend from SP ASIN data, deduplicated to latest load per date+asin
with source as (
    select * from {{ source('raw_amazon', 'sp_advertised_products') }}
),

deduplicated as (
    select
        date,
        advertised_asin as asin,
        cast(cost as float64) as cost,
        cast(impressions as int64) as impressions,
        cast(clicks as int64) as clicks,
        cast(sales14d as float64) as sales14d,
        _loaded_at
    from source
    qualify dense_rank() over(partition by date, advertised_asin order by _loaded_at desc) = 1
)

select
    date,
    asin,
    coalesce(cost, 0) as cost,
    impressions,
    clicks,
    sales14d as ad_sales
from deduplicated

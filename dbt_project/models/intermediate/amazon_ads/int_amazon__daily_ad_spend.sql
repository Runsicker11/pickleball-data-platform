with ad_spend as (
    select * from {{ ref('stg_amazon__ad_spend') }}
),

aggregated as (
    select
        date,
        asin,
        sum(cost) as cost,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(ad_sales) as ad_sales
    from ad_spend
    group by 1, 2
)

select * from aggregated

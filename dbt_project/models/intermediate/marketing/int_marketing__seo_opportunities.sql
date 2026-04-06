-- Striking-distance keywords (position 5-20)
-- Replaces: vw_seo_opportunities

with recent as (
    select
        site,
        query,
        page,
        avg(position) as avg_position,
        sum(impressions) as impressions_30d,
        sum(clicks) as clicks_30d,
        safe_divide(sum(clicks), sum(impressions)) as ctr
    from {{ ref('stg_search_console__performance') }}
    where query_date >= date_sub(current_date(), interval 30 day)
    group by site, query, page
)

select
    site,
    query,
    page,
    avg_position,
    impressions_30d,
    clicks_30d,
    ctr,
    round(impressions_30d * (1.0 / avg_position) * 10, 2) as opportunity_score
from recent
where avg_position between 5 and 20
    and impressions_30d >= 100
order by opportunity_score desc

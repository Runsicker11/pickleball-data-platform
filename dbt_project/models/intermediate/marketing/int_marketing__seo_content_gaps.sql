-- Pages with high impressions but low CTR
-- Replaces: vw_seo_content_gaps

with page_summary as (
    select
        site,
        page,
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        safe_divide(sum(clicks), sum(impressions)) as avg_ctr,
        avg(position) as avg_position
    from {{ ref('stg_search_console__performance') }}
    where query_date >= date_sub(current_date(), interval 30 day)
    group by site, page
    having sum(impressions) >= 200
)

select
    site,
    page,
    total_impressions,
    total_clicks,
    avg_ctr,
    avg_position,
    case
        when avg_position <= 5 and avg_ctr < 0.05
            then 'optimize_title'
        when avg_position between 5 and 10 and avg_ctr < 0.03
            then 'optimize_meta'
        when avg_position > 10 and avg_ctr < 0.02
            then 'review_content'
        else 'monitor'
    end as suggested_action
from page_summary
where avg_ctr < 0.05
order by total_impressions desc

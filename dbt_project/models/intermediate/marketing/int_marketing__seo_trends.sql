{{ config(enabled=false) }}
-- DISABLED: upstream stg_search_console__performance is disabled
-- Week-over-week ranking changes
-- Replaces: vw_seo_trends

with weekly as (
    select
        site,
        query,
        page,
        date_trunc(query_date, week(monday)) as week_start,
        avg(position) as avg_position,
        sum(clicks) as total_clicks,
        sum(impressions) as total_impressions
    from {{ ref('stg_search_console__performance') }}
    where query_date >= date_sub(current_date(), interval 28 day)
    group by site, query, page, week_start
),

with_prior as (
    select
        site,
        query,
        page,
        week_start,
        avg_position as current_week_position,
        lag(avg_position) over (
            partition by site, query, page order by week_start
        ) as prior_week_position,
        total_clicks as current_week_clicks,
        total_impressions as current_week_impressions
    from weekly
)

select
    site,
    query,
    page,
    week_start,
    current_week_position,
    prior_week_position,
    round(current_week_position - prior_week_position, 2) as position_change,
    current_week_clicks,
    current_week_impressions,
    case
        when prior_week_position is null then 'new'
        when current_week_position - prior_week_position <= -1 then 'improving'
        when current_week_position - prior_week_position >= 1 then 'declining'
        else 'stable'
    end as trend
from with_prior
where prior_week_position is not null
order by abs(current_week_position - prior_week_position) desc

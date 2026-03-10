{{ config(enabled=false) }}
-- DISABLED: upstream stg_search_console__performance is disabled
-- Published content tracking with Search Console + GA4 data
-- Replaces: vw_content_performance

with sc_by_url as (
    select
        page,
        avg(position) as current_position,
        sum(case when query_date >= date_sub(current_date(), interval 7 day) then impressions else 0 end) as impressions_7d,
        sum(case when query_date >= date_sub(current_date(), interval 7 day) then clicks else 0 end) as clicks_7d,
        safe_divide(
            sum(case when query_date >= date_sub(current_date(), interval 7 day) then clicks else 0 end),
            sum(case when query_date >= date_sub(current_date(), interval 7 day) then impressions else 0 end)
        ) as ctr
    from {{ ref('stg_search_console__performance') }}
    where query_date >= date_sub(current_date(), interval 30 day)
    group by page
),

ga4_by_page as (
    select
        concat('https://', (select value.string_value from unnest(event_params) where key = 'page_location')) as page_url,
        count(distinct case when event_name = 'session_start' then user_pseudo_id end) as ga4_sessions,
        count(distinct case when event_name = 'purchase' then user_pseudo_id end) as ga4_conversions,
        sum(case when event_name = 'purchase' then ecommerce.purchase_revenue else 0 end) as ga4_revenue
    from {{ source('ga4', 'events_*') }}
    where _table_suffix >= format_date('%Y%m%d', date_sub(current_date(), interval 30 day))
        and event_name in ('session_start', 'purchase')
    group by page_url
)

select
    cp.post_id,
    cp.title,
    cp.target_keyword,
    cp.content_type,
    cp.platform,
    cp.status,
    cp.url,
    cp.word_count,
    cp.publish_date,
    date_diff(current_date(), cp.publish_date, day) as days_since_publish,
    sc.current_position,
    sc.impressions_7d,
    sc.clicks_7d,
    sc.ctr,
    g.ga4_sessions,
    g.ga4_conversions,
    g.ga4_revenue,
    case
        when sc.clicks_7d >= 50 and sc.current_position <= 5 then 'top_performer'
        when sc.clicks_7d >= 20 and sc.current_position <= 10 then 'good'
        when sc.impressions_7d >= 100 and sc.clicks_7d < 5 then 'underperforming'
        when date_diff(current_date(), cp.publish_date, day) < 14 then 'too_early'
        else 'monitor'
    end as performance_tier
from {{ source('marketing_app', 'content_posts') }} cp
left join sc_by_url sc on cp.url = sc.page
left join ga4_by_page g on cp.url = g.page_url

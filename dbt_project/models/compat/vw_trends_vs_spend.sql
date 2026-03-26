-- Weekly trend interest vs paid spend — identifies under/overspend during peak demand periods
with trends as (
    select
        week,
        keyword,
        interest_score,
        is_partial
    from {{ ref('stg_google_trends__interest_over_time') }}
    where not is_partial
),

-- Aggregate daily Google Ads spend to weekly
spend as (
    select
        date_trunc(date_start, week) as week,
        campaign_name,
        sum(spend) as weekly_spend,
        sum(impressions) as weekly_impressions,
        sum(clicks) as weekly_clicks,
        sum(conversions) as weekly_conversions,
        sum(conversion_value) as weekly_conversion_value
    from {{ ref('stg_google_ads__daily_insights') }}
    group by 1, 2
),

-- Total weekly spend across all campaigns
total_spend as (
    select
        week,
        sum(weekly_spend) as total_weekly_spend,
        sum(weekly_impressions) as total_weekly_impressions,
        sum(weekly_clicks) as total_weekly_clicks,
        sum(weekly_conversions) as total_weekly_conversions,
        sum(weekly_conversion_value) as total_weekly_conversion_value
    from spend
    group by 1
)

select
    t.week,
    t.keyword,
    t.interest_score,
    s.total_weekly_spend,
    s.total_weekly_impressions,
    s.total_weekly_clicks,
    s.total_weekly_conversions,
    s.total_weekly_conversion_value,
    safe_divide(t.interest_score, s.total_weekly_spend) as interest_per_spend_dollar
from trends t
left join total_spend s using (week)

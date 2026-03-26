-- Ad schedule (dayparting) performance — identifies which hours and days drive conversions vs. waste spend
select
    date_start as date,
    campaign_id,
    campaign_name,
    criterion_id,
    day_of_week,
    start_hour,
    end_hour,
    start_minute,
    end_minute,
    impressions,
    clicks,
    cost,
    conversions,
    conversions_value,
    safe_divide(conversions_value, nullif(cost, 0)) as roas,
    safe_divide(cost, nullif(conversions, 0)) as cpa,
    ingested_at
from {{ ref('stg_google_ads__ad_schedule_view') }}

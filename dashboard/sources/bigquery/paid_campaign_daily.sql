select
    report_date,
    platform,
    campaign_id,
    campaign_name,
    adset_name,
    impressions,
    clicks,
    spend,
    cpc,
    ctr,
    conversions,
    conversion_value
from `practical-gecko-373320`.int_marketing.int_marketing__daily_performance
order by report_date desc, platform, campaign_name

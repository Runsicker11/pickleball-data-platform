select
    report_date,
    channel,
    spend,
    impressions,
    clicks,
    conversions,
    revenue,
    roas,
    cpa,
    orders
from `practical-gecko-373320`.bi.fct_marketing__channel_summary
order by report_date desc, channel

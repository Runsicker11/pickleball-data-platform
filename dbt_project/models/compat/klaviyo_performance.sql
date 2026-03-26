-- Daily Klaviyo email performance metrics for trend analysis and LTV attribution
select
    date,
    'store' as account,
    metric_name,
    value,
    ingested_at
from {{ ref('stg_klaviyo__metrics_timeline') }}

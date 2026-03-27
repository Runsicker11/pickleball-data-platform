-- Klaviyo campaign performance summary for email marketing analysis and Customer Match seeding
select
    c.campaign_id,
    c.campaign_name,
    'store' as account,
    cast(c.send_time as date) as send_date,
    m.recipients,
    m.delivered,
    m.opens,
    safe_divide(m.opens, nullif(m.delivered, 0)) as open_rate,
    m.clicks,
    safe_divide(m.clicks, nullif(m.delivered, 0)) as click_rate,
    m.unsubscribes,
    safe_divide(m.unsubscribes, nullif(m.delivered, 0)) as unsubscribe_rate,
    m.conversions,
    m.revenue,
    safe_divide(m.revenue, nullif(m.delivered, 0)) as revenue_per_recipient,
    m.ingested_at
from {{ ref('stg_klaviyo__campaigns') }} c
left join {{ ref('stg_klaviyo__campaign_metrics') }} m on c.campaign_id = m.campaign_id
where c.status = 'Sent'

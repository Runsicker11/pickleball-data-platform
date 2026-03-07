-- Creative text + performance metrics
-- Replaces: vw_creative_performance

with creative_daily as (
    select
        i.ad_id,
        i.ad_name,
        sum(i.spend) as lifetime_spend,
        sum(i.impressions) as lifetime_impressions,
        sum(i.clicks) as lifetime_clicks,
        sum(i.purchases) as lifetime_purchases,
        sum(i.purchase_value) as lifetime_revenue,
        safe_divide(sum(i.purchase_value), sum(i.spend)) as lifetime_roas,
        safe_divide(sum(i.clicks), sum(i.impressions)) * 100 as lifetime_ctr,
        safe_divide(sum(i.spend), sum(i.purchases)) as lifetime_cpa,
        min(i.date_start) as first_date,
        max(i.date_start) as last_date,
        count(distinct i.date_start) as days_active
    from {{ ref('stg_meta__daily_insights') }} i
    where i.spend > 0
    group by i.ad_id, i.ad_name
)

select
    c.creative_id,
    c.ad_id,
    cd.ad_name,
    c.title as headline,
    c.body as primary_text,
    c.cta_type,
    c.object_type,
    c.video_id,
    cd.lifetime_spend,
    cd.lifetime_impressions,
    cd.lifetime_clicks,
    cd.lifetime_purchases,
    cd.lifetime_revenue,
    cd.lifetime_roas,
    cd.lifetime_ctr,
    cd.lifetime_cpa,
    cd.first_date,
    cd.last_date,
    cd.days_active,
    case
        when cd.lifetime_roas >= 3.0 then 'top_performer'
        when cd.lifetime_roas >= 2.0 then 'good'
        when cd.lifetime_roas >= 1.0 then 'marginal'
        else 'underperformer'
    end as performance_tier,
    case
        when cd.days_active >= 14
            and cd.lifetime_ctr < 1.0 then 'possible_fatigue'
        when cd.lifetime_roas < 0.5
            and cd.lifetime_spend > 50 then 'cut_candidate'
        else 'healthy'
    end as health_status
from {{ ref('stg_meta__creatives') }} c
left join creative_daily cd on c.ad_id = cd.ad_id

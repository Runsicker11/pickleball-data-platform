-- Keyword performance with quality scores
-- Replaces: vw_google_ads_keywords

with keyword_perf as (
    select
        st.keyword_text,
        st.campaign_id,
        st.campaign_name,
        st.ad_group_id,
        st.ad_group_name,
        sum(st.impressions) as total_impressions,
        sum(st.clicks) as total_clicks,
        sum(st.spend) as total_spend,
        sum(st.conversions) as total_conversions,
        sum(st.conversion_value) as total_conversion_value,
        safe_divide(sum(st.clicks), sum(st.impressions)) as avg_ctr,
        safe_divide(sum(st.spend), sum(st.conversions)) as avg_cpa,
        safe_divide(sum(st.conversion_value), sum(st.spend)) as roas,
        count(distinct st.date_start) as days_active
    from {{ ref('stg_google_ads__search_terms') }} st
    group by st.keyword_text, st.campaign_id, st.campaign_name, st.ad_group_id, st.ad_group_name
)

select
    kp.keyword_text,
    kp.campaign_id,
    kp.campaign_name,
    kp.ad_group_id,
    kp.ad_group_name,
    kp.total_impressions,
    kp.total_clicks,
    kp.total_spend,
    kp.total_conversions,
    kp.total_conversion_value,
    kp.avg_ctr,
    kp.avg_cpa,
    kp.roas,
    kp.days_active,
    k.quality_score,
    k.expected_ctr,
    k.ad_relevance,
    k.landing_page_experience,
    case
        when kp.roas >= 3.0 and kp.total_conversions >= 3 then 'top_performer'
        when kp.roas >= 2.0 and kp.total_conversions >= 1 then 'good'
        when kp.roas >= 1.0 then 'marginal'
        when kp.total_spend > 10 and kp.total_conversions = 0 then 'wasted_spend'
        else 'underperformer'
    end as performance_tier
from keyword_perf kp
left join {{ ref('stg_google_ads__keywords') }} k
    on kp.keyword_text = k.keyword_text
    and kp.ad_group_id = k.ad_group_id

-- Asset-level performance for Google RSA headlines and descriptions
-- Replaces: vw_google_ads_copy_performance

select
    ac.ad_id,
    ac.campaign_id,
    ac.campaign_name,
    ac.ad_group_id,
    ac.ad_group_name,
    ac.ad_type,
    ac.asset_type,
    ac.asset_text,
    ac.performance_label,
    sum(gi.spend) as ad_group_spend,
    sum(gi.impressions) as ad_group_impressions,
    sum(gi.clicks) as ad_group_clicks,
    sum(gi.conversions) as ad_group_conversions,
    sum(gi.conversion_value) as ad_group_conversion_value,
    safe_divide(sum(gi.conversion_value), sum(gi.spend)) as ad_group_roas,
    safe_divide(sum(gi.clicks), sum(gi.impressions)) as ad_group_ctr
from {{ source('marketing_app', 'google_ads_ad_copy') }} ac
left join {{ ref('stg_google_ads__daily_insights') }} gi
    on ac.ad_group_id = gi.ad_group_id
    and gi.date_start >= date_sub(current_date(), interval 30 day)
group by ac.ad_id, ac.campaign_id, ac.campaign_name, ac.ad_group_id,
         ac.ad_group_name, ac.ad_type, ac.asset_type, ac.asset_text,
         ac.performance_label

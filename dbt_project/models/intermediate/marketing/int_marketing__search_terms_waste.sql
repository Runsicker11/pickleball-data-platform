-- Search terms with spend but zero conversions — negative keyword candidates
-- Replaces: vw_search_terms_waste

select
    search_term,
    sum(impressions) as total_impressions,
    sum(clicks) as total_clicks,
    sum(spend) as total_spend,
    sum(conversions) as total_conversions,
    safe_divide(sum(clicks), sum(impressions)) as avg_ctr,
    count(distinct date_start) as days_seen,
    array_agg(distinct campaign_name ignore nulls) as campaigns,
    array_agg(distinct keyword_text ignore nulls) as matched_keywords
from {{ ref('stg_google_ads__search_terms') }}
where spend > 0
group by search_term
having sum(conversions) = 0
    and sum(spend) >= 5

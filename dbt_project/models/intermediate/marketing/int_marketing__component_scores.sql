-- Content library component performance scoring
-- Replaces: vw_component_scores

with component_matches as (
    select
        cl.component_id,
        cl.component_type,
        cl.text,
        cp.creative_id,
        cp.ad_id,
        cp.ad_name,
        cp.lifetime_roas,
        cp.lifetime_ctr,
        cp.lifetime_cpa,
        cp.lifetime_spend,
        cp.lifetime_purchases,
        cp.days_active
    from {{ source('marketing_app', 'content_library') }} cl
    join {{ ref('int_marketing__creative_performance') }} cp
        on (cl.component_type = 'hook' and cp.headline is not null
            and lower(trim(cl.text)) = lower(trim(cp.headline)))
        or (cl.component_type = 'body' and cp.primary_text is not null
            and lower(trim(cl.text)) = lower(trim(cp.primary_text)))
    where cp.days_active >= 7
)

select
    component_id,
    component_type,
    text,
    count(distinct ad_id) as ads_using,
    sum(lifetime_spend) as total_spend,
    sum(lifetime_purchases) as total_purchases,
    safe_divide(sum(lifetime_purchases * lifetime_roas * lifetime_spend),
                sum(lifetime_spend)) as weighted_roas,
    avg(lifetime_ctr) as avg_ctr,
    avg(lifetime_cpa) as avg_cpa,
    max(days_active) as max_days_active
from component_matches
group by component_id, component_type, text

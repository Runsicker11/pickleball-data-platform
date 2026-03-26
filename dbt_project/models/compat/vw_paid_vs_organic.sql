-- Paid vs organic keyword overlap — identifies bid waste and SEO opportunities
with organic as (
    select
        query_date,
        site,
        query,
        page,
        device,
        country,
        clicks as organic_clicks,
        impressions as organic_impressions,
        ctr as organic_ctr,
        position as organic_position
    from {{ ref('stg_search_console__performance') }}
),

paid as (
    select
        date_start,
        search_term,
        campaign_id,
        campaign_name,
        sum(clicks) as paid_clicks,
        sum(impressions) as paid_impressions,
        sum(spend) as paid_spend,
        sum(conversions) as paid_conversions,
        sum(conversion_value) as paid_conversion_value
    from {{ ref('stg_google_ads__search_terms') }}
    group by 1, 2, 3, 4
),

joined as (
    select
        coalesce(o.query_date, p.date_start) as report_date,
        coalesce(o.query, p.search_term) as query,
        o.site,
        o.page,
        o.device,
        o.country,
        o.organic_clicks,
        o.organic_impressions,
        o.organic_ctr,
        o.organic_position,
        p.campaign_id,
        p.campaign_name,
        p.paid_clicks,
        p.paid_impressions,
        p.paid_spend,
        p.paid_conversions,
        p.paid_conversion_value,
        case
            when o.query is not null and p.search_term is not null and o.organic_position <= 3
                then 'strong_organic_and_paid'
            when o.query is not null and p.search_term is not null and o.organic_position between 4 and 10
                then 'weak_organic_and_paid'
            when o.query is not null and p.search_term is not null
                then 'organic_and_paid'
            when o.query is not null and p.search_term is null
                then 'organic_only'
            else 'paid_only'
        end as overlap_type
    from organic o
    full outer join paid p
        on o.query = p.search_term
        and o.query_date = p.date_start
)

select * from joined

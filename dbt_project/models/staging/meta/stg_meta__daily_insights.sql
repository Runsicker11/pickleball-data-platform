with source as (
    select * from {{ source('raw_meta', 'daily_insights') }}
),

renamed as (
    select
        cast(date_start as date) as date_start,
        cast(campaign_id as string) as campaign_id,
        cast(adset_id as string) as adset_id,
        cast(ad_id as string) as ad_id,
        campaign_name,
        adset_name,
        ad_name,
        cast(impressions as int64) as impressions,
        cast(clicks as int64) as clicks,
        cast(spend as float64) as spend,
        cast(cpc as float64) as cpc,
        cast(cpm as float64) as cpm,
        cast(ctr as float64) as ctr,
        cast(reach as int64) as reach,
        cast(frequency as float64) as frequency,
        cast(purchases as int64) as purchases,
        cast(add_to_cart as int64) as add_to_cart,
        cast(initiate_checkout as int64) as initiate_checkout,
        cast(landing_page_views as int64) as landing_page_views,
        cast(link_clicks as int64) as link_clicks,
        cast(purchase_value as float64) as purchase_value,
        cast(add_to_cart_value as float64) as add_to_cart_value,
        cast(initiate_checkout_value as float64) as initiate_checkout_value

    from source
)

select * from renamed

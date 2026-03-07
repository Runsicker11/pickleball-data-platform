-- DISABLED: Search Console refresh_token not yet configured in pipeline-env secret.
-- Re-enable once GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN is added to .env.
{{ config(enabled=false) }}

with source as (
    select * from {{ source('raw_search_console', 'performance') }}
),

renamed as (
    select
        cast(query_date as date) as query_date,
        site,
        query,
        page,
        country,
        device,
        cast(impressions as int64) as impressions,
        cast(clicks as int64) as clicks,
        cast(ctr as float64) as ctr,
        cast(position as float64) as position

    from source
)

select * from renamed

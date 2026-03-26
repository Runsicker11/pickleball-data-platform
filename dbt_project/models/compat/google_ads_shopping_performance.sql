-- Product-level shopping performance for AI recommendation system
select * from {{ ref('stg_google_ads__shopping_performance') }}

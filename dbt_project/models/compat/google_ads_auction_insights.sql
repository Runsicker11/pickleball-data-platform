-- Competitor auction insights for AI recommendation system — who is bidding on our keywords
select * from {{ ref('stg_google_ads__auction_insights') }}

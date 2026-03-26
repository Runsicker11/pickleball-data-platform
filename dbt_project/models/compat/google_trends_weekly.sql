-- Weekly Google Trends interest scores for pickleball keywords — seasonality signal for bid recommendations
select
    week,
    keyword,
    interest_score,
    is_partial
from {{ ref('stg_google_trends__interest_over_time') }}

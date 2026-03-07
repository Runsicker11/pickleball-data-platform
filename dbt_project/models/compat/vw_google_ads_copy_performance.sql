-- Compatibility view for ai-marketing
select * from {{ ref('int_marketing__google_ads_copy_performance') }}

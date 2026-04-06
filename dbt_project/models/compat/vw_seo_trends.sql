-- Compatibility view for ai-marketing: week-over-week ranking changes
select * from {{ ref('int_marketing__seo_trends') }}

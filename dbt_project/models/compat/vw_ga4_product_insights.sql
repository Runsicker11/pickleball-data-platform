-- Compatibility view for ai-marketing
select * from {{ ref('int_marketing__ga4_product_insights') }}

-- Compatibility view for ai-marketing
select * from {{ ref('int_marketing__product_performance') }}

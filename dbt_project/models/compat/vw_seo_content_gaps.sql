-- Compatibility view for ai-marketing: high-impression low-CTR pages
select * from {{ ref('int_marketing__seo_content_gaps') }}

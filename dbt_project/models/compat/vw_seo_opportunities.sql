-- Compatibility view for ai-marketing: striking-distance keyword opportunities
select * from {{ ref('int_marketing__seo_opportunities') }}

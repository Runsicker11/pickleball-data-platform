-- Compatibility view for ai-marketing
select * from {{ ref('int_marketing__search_terms_waste') }}

{{ config(enabled=false) }}
-- DISABLED: upstream stg_search_console__performance is disabled
-- Compatibility view for ai-marketing
select * from {{ ref('int_marketing__seo_content_gaps') }}

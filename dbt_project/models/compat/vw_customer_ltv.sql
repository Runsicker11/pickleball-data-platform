-- Compatibility view for ai-marketing (points to materialized mart)
select * from {{ ref('fct_marketing__customer_ltv') }}

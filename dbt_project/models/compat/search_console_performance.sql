-- Search Console organic performance for paid/organic overlap analysis
select * from {{ ref('stg_search_console__performance') }}

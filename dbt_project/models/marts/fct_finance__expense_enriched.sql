{{
  config(
    materialized='table'
  )
}}

select * from {{ ref('int_finance__expense_enriched') }}

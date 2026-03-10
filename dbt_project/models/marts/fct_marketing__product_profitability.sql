{{
  config(
    materialized='table'
  )
}}

select * from {{ ref('int_marketing__product_profitability') }}

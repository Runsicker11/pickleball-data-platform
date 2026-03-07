{{
  config(
    materialized='table'
  )
}}

select * from {{ ref('int_marketing__enhanced_roas') }}

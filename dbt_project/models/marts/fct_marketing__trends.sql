{{
  config(
    materialized='table'
  )
}}

select * from {{ ref('int_marketing__trends') }}

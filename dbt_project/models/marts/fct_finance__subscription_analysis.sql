{{
  config(
    materialized='table'
  )
}}

select * from {{ ref('int_finance__subscription_analysis') }}

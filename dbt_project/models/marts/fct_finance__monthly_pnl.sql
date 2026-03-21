{{
  config(
    materialized='table'
  )
}}

select * from {{ ref('int_finance__monthly_pnl') }}

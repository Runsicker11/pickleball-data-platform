{{ config(materialized='table') }}

select * from {{ ref('int_finance__affiliate_by_partner') }}

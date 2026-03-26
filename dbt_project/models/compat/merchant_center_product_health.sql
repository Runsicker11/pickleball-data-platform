-- Product feed health for AI recommendation system — flags disapproved products wasting ad spend
with products as (
    select
        product_id,
        title,
        availability,
        price_value as price,
        brand,
        ingested_at
    from {{ ref('stg_merchant_center__products') }}
),

statuses as (
    select
        product_id,
        approval_status,
        disapproval_reasons
    from {{ ref('stg_merchant_center__product_statuses') }}
    -- One row per product (first row if multiple issues collapsed to disapproval_reasons string)
    qualify row_number() over (partition by product_id order by ingested_at desc) = 1
)

select
    p.product_id,
    p.title,
    p.availability,
    p.price,
    p.brand,
    coalesce(s.approval_status, 'unknown') as approval_status,
    coalesce(s.disapproval_reasons, '') as disapproval_reasons,
    p.ingested_at
from products p
left join statuses s using (product_id)

-- Product sales by channel
-- Replaces: vw_product_performance

select
    li.product_id,
    coalesce(p.title, li.title) as product_title,
    p.product_type,
    coalesce(o.utm_source, 'direct') as source,
    coalesce(o.utm_medium, 'none') as medium,
    o.utm_campaign as campaign,
    count(distinct o.order_id) as orders,
    sum(li.quantity) as units_sold,
    sum(li.price * li.quantity) as gross_revenue,
    sum(li.total_discount) as total_discounts,
    sum(li.price * li.quantity - li.total_discount) as net_revenue,
    avg(li.price) as avg_price
from {{ ref('stg_shopify__order_line_items') }} li
join {{ ref('stg_shopify__orders') }} o on li.order_id = o.order_id
left join {{ ref('stg_shopify__products') }} p on li.product_id = p.product_id
where o.financial_status not in ('refunded', 'voided')
group by li.product_id, product_title, p.product_type, source, medium, campaign

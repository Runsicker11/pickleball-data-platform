-- Customer cohort assignment with order-level profitability
-- Each row = one customer-order with gross profit and cohort metadata

with orders as (
    select
        order_id,
        customer_id,
        order_date
    from {{ ref('stg_shopify__orders') }}
    where customer_id is not null
),

line_items as (
    select
        order_id,
        sku,
        title,
        quantity,
        price,
        total_discount,
        title = 'Tuning Clamps' as is_partnership,
        case when title = 'Tuning Clamps'
             then (cast(price as float64) * quantity - total_discount) * 0.40
             else cast(price as float64) * quantity - total_discount
        end as net_revenue
    from {{ ref('stg_shopify__order_line_items') }}
    where sku is not null and sku != ''
),

cogs_lookup as (
    select
        lower(trim(sku)) as sku_key,
        avg(cogs) as unit_cost
    from {{ source('product', 'amazon_product_map') }}
    where sku is not null and cogs is not null
    group by sku_key
),

order_profitability as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        sum(li.net_revenue) as net_revenue,
        sum(
            case
                when li.is_partnership then 0
                when c.unit_cost is not null then c.unit_cost * li.quantity
                else li.net_revenue * 0.40
            end
        ) as total_cogs,
        sum(li.net_revenue) - sum(
            case
                when li.is_partnership then 0
                when c.unit_cost is not null then c.unit_cost * li.quantity
                else li.net_revenue * 0.40
            end
        ) as gross_profit,
        5.35 as estimated_shipping_cost,
        sum(li.net_revenue) - sum(
            case
                when li.is_partnership then 0
                when c.unit_cost is not null then c.unit_cost * li.quantity
                else li.net_revenue * 0.40
            end
        ) - 5.35 as contribution_profit
    from orders o
    join line_items li on o.order_id = li.order_id
    left join cogs_lookup c on lower(trim(li.sku)) = c.sku_key
    group by o.order_id, o.customer_id, o.order_date
),

customer_first_order as (
    select
        customer_id,
        min(order_date) as first_order_date,
        date_trunc(min(order_date), month) as cohort_month
    from order_profitability
    group by customer_id
)

select
    op.order_id,
    op.customer_id,
    op.order_date,
    cfo.first_order_date,
    cfo.cohort_month,
    date_diff(op.order_date, cfo.first_order_date, month) as months_since_first_order,
    op.net_revenue,
    op.total_cogs,
    op.gross_profit,
    sum(op.gross_profit) over (
        partition by op.customer_id
        order by op.order_date
        rows between unbounded preceding and current row
    ) as cumulative_gross_profit,
    op.estimated_shipping_cost,
    op.contribution_profit,
    sum(op.contribution_profit) over (
        partition by op.customer_id
        order by op.order_date
        rows between unbounded preceding and current row
    ) as cumulative_contribution_profit
from order_profitability op
join customer_first_order cfo on op.customer_id = cfo.customer_id

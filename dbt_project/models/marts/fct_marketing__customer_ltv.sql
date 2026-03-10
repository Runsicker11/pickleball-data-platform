-- Customer Lifetime Value by cohort
-- Averages 6-month cumulative gross profit across customers mature enough to measure

with customer_orders as (
    select * from {{ ref('int_marketing__customer_cohorts') }}
),

customer_6mo_profit as (
    select
        customer_id,
        cohort_month,
        first_order_date,
        count(distinct order_id) as orders_in_6mo,
        sum(gross_profit) as gross_profit_6mo,
        sum(net_revenue) as revenue_6mo,
        sum(contribution_profit) as contribution_profit_6mo,
        sum(estimated_shipping_cost) as shipping_cost_6mo
    from customer_orders
    where months_since_first_order < 6
    group by customer_id, cohort_month, first_order_date
),

-- only include customers who have had 6+ months to mature
mature_customers as (
    select *
    from customer_6mo_profit
    where date_diff(current_date(), first_order_date, month) >= 6
)

select
    cohort_month,
    count(distinct customer_id) as cohort_size,
    avg(gross_profit_6mo) as avg_ltv_6mo,
    avg(revenue_6mo) as avg_revenue_6mo,
    avg(orders_in_6mo) as avg_orders_6mo,
    sum(gross_profit_6mo) as total_gross_profit_6mo,
    sum(revenue_6mo) as total_revenue_6mo,
    safe_divide(
        sum(gross_profit_6mo),
        sum(revenue_6mo)
    ) as cohort_gross_margin,
    avg(contribution_profit_6mo) as avg_contribution_ltv_6mo,
    sum(contribution_profit_6mo) as total_contribution_profit_6mo,
    sum(shipping_cost_6mo) as total_shipping_cost_6mo,
    safe_divide(
        sum(contribution_profit_6mo),
        sum(revenue_6mo)
    ) as cohort_contribution_margin
from mature_customers
group by cohort_month
order by cohort_month

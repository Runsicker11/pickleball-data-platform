select
    order_id,
    customer_id,
    order_date,
    first_order_date,
    cohort_month,
    months_since_first_order,
    net_revenue,
    gross_profit,
    cumulative_gross_profit,
    contribution_profit,
    cumulative_contribution_profit,
    estimated_shipping_cost
from `practical-gecko-373320`.int_marketing.int_marketing__customer_cohorts
order by customer_id, order_date

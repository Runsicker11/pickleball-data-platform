select
    cohort_month,
    cohort_size,
    avg_ltv_6mo,
    avg_revenue_6mo,
    avg_orders_6mo,
    avg_contribution_ltv_6mo,
    total_revenue_6mo,
    total_gross_profit_6mo,
    total_contribution_profit_6mo,
    cohort_gross_margin,
    cohort_contribution_margin,
    total_shipping_cost_6mo
from `practical-gecko-373320`.bi.fct_marketing__customer_ltv
order by cohort_month desc

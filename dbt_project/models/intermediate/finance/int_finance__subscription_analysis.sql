-- Subscription deep-dive: cost projections + cost-cutting signals

with vendor_months as (
    select
        vendor_name,
        month,
        total_spend,
        transaction_count,
        is_likely_subscription
    from {{ ref('int_finance__expense_by_vendor') }}
    where is_likely_subscription = true
),

vendor_summary as (
    select
        vendor_name,
        avg(total_spend) as avg_monthly_cost,
        avg(total_spend) * 12 as annual_projection,
        count(distinct month) as months_active,
        min(month) as first_seen,
        max(month) as last_seen,
        sum(total_spend) as total_spend
    from vendor_months
    group by vendor_name
),

-- Compute early vs recent cost for price-creep detection
early_avg as (
    select
        vendor_name,
        avg(total_spend) as early_cost
    from (
        select
            vendor_name,
            total_spend,
            row_number() over (partition by vendor_name order by month) as rn
        from vendor_months
    )
    where rn <= 3
    group by vendor_name
),

recent_avg as (
    select
        vendor_name,
        avg(total_spend) as recent_cost
    from (
        select
            vendor_name,
            total_spend,
            row_number() over (partition by vendor_name order by month desc) as rn
        from vendor_months
    )
    where rn <= 3
    group by vendor_name
)

select
    vs.vendor_name,
    vs.avg_monthly_cost,
    vs.annual_projection,
    vs.months_active,
    vs.first_seen,
    vs.last_seen,
    vs.total_spend,
    case
        when ra.recent_cost > ea.early_cost * 1.10
            and vs.annual_projection > 1000
            then 'Price creeping up'
        when vs.last_seen < date_trunc(current_date(), month) - interval 1 month
            and vs.annual_projection > 500
            then 'May be unused'
        when vs.annual_projection > 2000
            then 'High-cost — review'
        else null
    end as cost_cutting_signal
from vendor_summary vs
left join early_avg ea on vs.vendor_name = ea.vendor_name
left join recent_avg ra on vs.vendor_name = ra.vendor_name

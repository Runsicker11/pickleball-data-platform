-- Vendor spend analysis with subscription detection

with vendor_monthly as (
    select
        vendor_name,
        date_trunc(txn_date, month) as month,
        sum(amount) as total_spend,
        count(*) as transaction_count,
        avg(amount) as avg_amount
    from {{ ref('stg_quickbooks__purchase_line_items') }}
    where vendor_name is not null
        and txn_date is not null
    group by 1, 2
),

-- Detect likely subscriptions: vendor appears in 3+ months with similar amounts
vendor_stats as (
    select
        vendor_name,
        count(distinct month) as months_active,
        stddev(total_spend) as spend_stddev,
        avg(total_spend) as avg_monthly_spend
    from vendor_monthly
    group by 1
)

select
    vm.vendor_name,
    vm.month,
    vm.total_spend,
    vm.transaction_count,
    vm.avg_amount,
    -- Flag as likely subscription if: 3+ months AND low variance relative to mean
    case
        when vs.months_active >= 3
            and (vs.spend_stddev is null or safe_divide(vs.spend_stddev, vs.avg_monthly_spend) < 0.3)
        then true
        else false
    end as is_likely_subscription
from vendor_monthly vm
left join vendor_stats vs
    on vm.vendor_name = vs.vendor_name

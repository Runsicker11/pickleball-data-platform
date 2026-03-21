-- Monthly P&L: revenue streams vs total expenses = net income

with revenue as (
    select
        month,
        sum(revenue) as total_revenue,
        sum(case when revenue_stream in ('Wholesale', 'Shopify', 'Amazon') then revenue else 0 end) as retail_revenue,
        sum(case when revenue_stream = 'Wholesale' then revenue else 0 end) as wholesale_revenue,
        sum(case when revenue_stream = 'Shopify' then revenue else 0 end) as shopify_revenue,
        sum(case when revenue_stream = 'Amazon' then revenue else 0 end) as amazon_revenue,
        sum(case when revenue_stream = 'Affiliate' then revenue else 0 end) as affiliate_revenue,
        sum(case when revenue_stream = 'Sponsorship' then revenue else 0 end) as sponsorship_revenue
    from {{ ref('int_finance__monthly_revenue') }}
    group by 1
),

expenses as (
    select
        month,
        sum(total_amount) as total_expenses,
        sum(case when not is_owner_compensation then total_amount else 0 end) as operating_expenses,
        sum(case when is_owner_compensation then total_amount else 0 end) as owner_compensation
    from {{ ref('int_finance__monthly_expenses') }}
    group by 1
),

months as (
    select month from revenue
    union distinct
    select month from expenses
)

select
    m.month,
    coalesce(r.total_revenue, 0) as total_revenue,
    coalesce(r.retail_revenue, 0) as retail_revenue,
    coalesce(r.wholesale_revenue, 0) as wholesale_revenue,
    coalesce(r.shopify_revenue, 0) as shopify_revenue,
    coalesce(r.amazon_revenue, 0) as amazon_revenue,
    coalesce(r.affiliate_revenue, 0) as affiliate_revenue,
    coalesce(r.sponsorship_revenue, 0) as sponsorship_revenue,
    coalesce(e.total_expenses, 0) as total_expenses,
    coalesce(e.operating_expenses, 0) as operating_expenses,
    coalesce(e.owner_compensation, 0) as owner_compensation,
    coalesce(r.total_revenue, 0) - coalesce(e.total_expenses, 0) as net_income,
    coalesce(r.total_revenue, 0) - coalesce(e.operating_expenses, 0) as operating_income,
    safe_divide(
        coalesce(r.total_revenue, 0) - coalesce(e.operating_expenses, 0),
        coalesce(r.total_revenue, 0)
    ) as operating_margin
from months m
left join revenue r on m.month = r.month
left join expenses e on m.month = e.month
order by m.month

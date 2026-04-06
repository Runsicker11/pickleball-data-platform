---
title: Customer LTV
order: 5
---

<DateRange
    name=date_range
    defaultValue="Last 12 Months"
    presetRanges={['Last 3 Months', 'Last 6 Months', 'Last 12 Months', 'Last Year', 'Year to Date', 'All Time']}
/>

```sql ltv_kpi
select
    sum(cohort_size) as total_customers,
    sum(avg_ltv_6mo * cohort_size) / nullif(sum(cohort_size), 0) as avg_ltv_6mo,
    sum(avg_contribution_ltv_6mo * cohort_size) / nullif(sum(cohort_size), 0) as avg_contribution_ltv,
    avg(cohort_gross_margin) as avg_gross_margin,
    avg(cohort_contribution_margin) as avg_contribution_margin
from bigquery.cohort_ltv
where cohort_month >= '${inputs.date_range.start}'::date
  and cohort_month <= '${inputs.date_range.end}'::date
```

```sql repeat_kpi
with customers as (
    select
        customer_id,
        max(months_since_first_order) > 0 as is_repeat
    from bigquery.customer_cohorts
    where cohort_month >= '${inputs.date_range.start}'::date
      and cohort_month <= '${inputs.date_range.end}'::date
    group by 1
)
select
    count(*) as total_customers,
    sum(case when is_repeat then 1 else 0 end) * 1.0 / count(*) as repeat_rate,
    avg(case when is_repeat then 1 else null end) as has_repeat
from customers
```

```sql new_vs_repeat_monthly
select
    date_trunc('month', order_date) as month,
    case when months_since_first_order = 0 then 'New Customer' else 'Repeat Buyer' end as buyer_type,
    count(distinct customer_id) as customer_count
from bigquery.customer_cohorts
where order_date >= '${inputs.date_range.start}'::date
  and order_date <= '${inputs.date_range.end}'::date
group by 1, 2
order by month, buyer_type
```

```sql cohort_ltv_trend
select
    cohort_month,
    cohort_size,
    avg_ltv_6mo,
    avg_contribution_ltv_6mo,
    cohort_gross_margin,
    cohort_contribution_margin,
    avg_orders_6mo
from bigquery.cohort_ltv
where cohort_month >= '${inputs.date_range.start}'::date
  and cohort_month <= '${inputs.date_range.end}'::date
order by cohort_month
```

```sql cohort_repeat_rate
select
    cohort_month,
    count(distinct customer_id) as total_customers,
    count(distinct case when months_since_first_order > 0 then customer_id end) as repeat_customers,
    count(distinct case when months_since_first_order > 0 then customer_id end) * 100.0 /
        nullif(count(distinct customer_id), 0) as repeat_rate_pct,
    avg(case when months_since_first_order = 1 then months_since_first_order end) as months_to_second
from bigquery.customer_cohorts
where cohort_month >= '${inputs.date_range.start}'::date
  and cohort_month <= '${inputs.date_range.end}'::date
group by 1
order by cohort_month
```

```sql profitability_by_cohort
select
    cohort_month,
    sum(gross_profit) as total_gross_profit,
    sum(contribution_profit) as total_contribution_profit,
    sum(net_revenue) as total_revenue,
    sum(gross_profit) / nullif(sum(net_revenue), 0) as gross_margin,
    sum(contribution_profit) / nullif(sum(net_revenue), 0) as contribution_margin
from bigquery.customer_cohorts
where cohort_month >= '${inputs.date_range.start}'::date
  and cohort_month <= '${inputs.date_range.end}'::date
group by 1
order by cohort_month
```

<BigValue
    data={ltv_kpi}
    value=avg_ltv_6mo
    title="Avg 6-Month LTV (Revenue)"
    fmt=usd
/>

<BigValue
    data={ltv_kpi}
    value=avg_contribution_ltv
    title="Avg 6-Month Contribution LTV"
    fmt=usd
/>

<BigValue
    data={repeat_kpi}
    value=repeat_rate
    title="Repeat Buyer Rate"
    fmt=pct1
/>

<BigValue
    data={ltv_kpi}
    value=avg_gross_margin
    title="Avg Cohort Gross Margin"
    fmt=pct1
/>

## New vs Repeat Customers by Month

<BarChart
    data={new_vs_repeat_monthly}
    x=month
    y=customer_count
    series=buyer_type
    type=stacked
    title="New vs Repeat Customers by Month"
/>

## Avg 6-Month LTV by Acquisition Cohort

<LineChart
    data={cohort_ltv_trend}
    x=cohort_month
    y=avg_ltv_6mo
    title="Avg 6-Month Revenue LTV by Cohort"
    yFmt=usd
/>

## Contribution LTV vs Revenue LTV

<BarChart
    data={cohort_ltv_trend}
    x=cohort_month
    y={['avg_ltv_6mo', 'avg_contribution_ltv_6mo']}
    type=grouped
    title="Revenue LTV vs Contribution LTV by Cohort"
    yFmt=usd
/>

## Cohort Repeat Rate

<LineChart
    data={cohort_repeat_rate}
    x=cohort_month
    y=repeat_rate_pct
    title="Repeat Buyer Rate by Acquisition Cohort (%)"
/>

## Cohort Summary Table

<DataTable data={cohort_ltv_trend}>
    <Column id=cohort_month fmt=mmm-yy title="Cohort"/>
    <Column id=cohort_size fmt=num0 title="Customers"/>
    <Column id=avg_ltv_6mo fmt=usd title="Avg 6mo LTV"/>
    <Column id=avg_contribution_ltv_6mo fmt=usd title="Avg Contribution LTV"/>
    <Column id=avg_orders_6mo fmt=num1 title="Avg Orders"/>
    <Column id=cohort_gross_margin fmt=pct1 title="Gross Margin"/>
    <Column id=cohort_contribution_margin fmt=pct1 title="Contribution Margin"/>
</DataTable>

## Cohort Profitability Detail

<DataTable data={cohort_repeat_rate}>
    <Column id=cohort_month fmt=mmm-yy title="Cohort"/>
    <Column id=total_customers fmt=num0 title="Customers"/>
    <Column id=repeat_customers fmt=num0 title="Repeat Buyers"/>
    <Column id=repeat_rate_pct fmt=num1 title="Repeat Rate %"/>
</DataTable>

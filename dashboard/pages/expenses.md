---
title: Expenses
---

<DateRange
    name=date_range
    defaultValue="Last 12 Months"
    presetRanges={['Last Month', 'Last 3 Months', 'Last 6 Months', 'Last 12 Months', 'Last Year', 'Year to Date', 'All Time']}
/>

```sql enriched
select * from bigquery.expense_enriched
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
```

```sql subscriptions
select * from bigquery.subscription_analysis
```

```sql vendors
select * from bigquery.expense_by_vendor
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
```

```sql expense_kpi_comparison
select
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        and expense_bucket != 'Payroll & Benefits'
        then total_amount else 0 end) as total_expenses,
    sum(case when month >= ('${inputs.date_range.start}'::date - interval '12 months')
        and month < '${inputs.date_range.start}'
        and expense_bucket != 'Payroll & Benefits'
        then total_amount else 0 end) as prior_total_expenses,
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        and expense_bucket = 'Payroll & Benefits'
        then total_amount else 0 end) as owner_comp
from bigquery.expense_enriched
where month >= ('${inputs.date_range.start}'::date - interval '12 months')
    and month <= '${inputs.date_range.end}'
```

```sql sub_kpis
select
    count(*) as active_subscriptions,
    sum(avg_monthly_cost) as monthly_burn,
    sum(annual_projection) as annual_projection
from bigquery.subscription_analysis
```

```sql bucket_monthly
select
    month,
    expense_bucket,
    sum(total_amount) as total_amount
from bigquery.expense_enriched
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
group by 1, 2
order by month, expense_bucket
```

```sql top_categories
select
    expense_category,
    expense_bucket,
    sum(total_amount) as total_amount,
    sum(mom_change) as mom_change,
    avg(mom_change_pct) as avg_mom_change_pct
from bigquery.expense_enriched
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
group by 1, 2
order by total_amount desc
limit 20
```

```sql cost_cutting
select *
from bigquery.subscription_analysis
where cost_cutting_signal is not null
order by annual_projection desc
```

```sql top_sub_trend
select
    v.month,
    v.vendor_name,
    v.total_spend
from bigquery.expense_by_vendor v
inner join (
    select vendor_name
    from bigquery.subscription_analysis
    order by annual_projection desc
    limit 10
) top on v.vendor_name = top.vendor_name
where v.month >= '${inputs.date_range.start}' and v.month <= '${inputs.date_range.end}'
order by v.month, v.vendor_name
```

```sql top_vendors
select
    vendor_name,
    sum(total_spend) as total_spend,
    sum(transaction_count) as total_transactions,
    avg(total_spend) as avg_monthly_spend,
    max(is_likely_subscription) as is_subscription
from bigquery.expense_by_vendor
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
group by vendor_name
order by total_spend desc
limit 20
```

<BigValue
    data={expense_kpi_comparison}
    value=total_expenses
    comparison=prior_total_expenses
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    comparisonFmt=usd
    downIsGood={true}
    title="Operating Expenses"
    fmt=usd
/>

<BigValue
    data={expense_kpi_comparison}
    value=owner_comp
    title="Owner Compensation"
    fmt=usd
    downIsGood={true}
/>

<BigValue
    data={sub_kpis}
    value=active_subscriptions
    title="Active Subscriptions"
/>

<BigValue
    data={sub_kpis}
    value=monthly_burn
    title="Monthly Subscription Burn"
    fmt=usd
/>

<BigValue
    data={sub_kpis}
    value=annual_projection
    title="Annual Subscription Projection"
    fmt=usd
/>

## Expenses by Bucket

<BarChart
    data={bucket_monthly}
    x=month
    y=total_amount
    series=expense_bucket
    type=stacked
    title="Monthly Expenses by Bucket"
    yFmt=usd
/>

## Expense Trend by Bucket

<AreaChart
    data={bucket_monthly}
    x=month
    y=total_amount
    series=expense_bucket
    type=stacked
    title="Expense Composition Over Time"
    yFmt=usd
/>

## Top Expense Categories

<DataTable data={top_categories} rows=20>
    <Column id=expense_category title="Category"/>
    <Column id=expense_bucket title="Bucket"/>
    <Column id=total_amount fmt=usd title="Total Spend"/>
    <Column id=mom_change fmt=usd title="MoM Change"/>
    <Column id=avg_mom_change_pct fmt=pct1 title="Avg MoM %"/>
</DataTable>

## Subscription Deep Dive

### Active Subscriptions

<DataTable data={subscriptions} rows=30>
    <Column id=vendor_name title="Vendor"/>
    <Column id=avg_monthly_cost fmt=usd title="Avg Monthly"/>
    <Column id=annual_projection fmt=usd title="Annual Projection"/>
    <Column id=months_active title="Months Active"/>
    <Column id=first_seen fmt=mmm-yy title="First Seen"/>
    <Column id=last_seen fmt=mmm-yy title="Last Seen"/>
    <Column id=cost_cutting_signal title="Signal"/>
</DataTable>

### Cost-Cutting Opportunities

Subscriptions flagged for review — price increases, potential disuse, or high cost.

<DataTable data={cost_cutting}>
    <Column id=vendor_name title="Vendor"/>
    <Column id=cost_cutting_signal title="Signal"/>
    <Column id=avg_monthly_cost fmt=usd title="Avg Monthly"/>
    <Column id=annual_projection fmt=usd title="Annual Projection"/>
    <Column id=months_active title="Months"/>
    <Column id=last_seen fmt=mmm-yy title="Last Seen"/>
</DataTable>

### Top Subscription Trend

<LineChart
    data={top_sub_trend}
    x=month
    y=total_spend
    series=vendor_name
    title="Top 10 Subscriptions by Cost Over Time"
    yFmt=usd
/>

## Top Vendors by Spend

<DataTable data={top_vendors}>
    <Column id=vendor_name title="Vendor"/>
    <Column id=total_spend fmt=usd title="Total Spend"/>
    <Column id=total_transactions title="Transactions"/>
    <Column id=avg_monthly_spend fmt=usd title="Avg Monthly"/>
    <Column id=is_subscription title="Subscription?"/>
</DataTable>

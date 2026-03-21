---
title: Revenue
---

<DateRange
    name=date_range
    defaultValue="Last 12 Months"
    presetRanges={['Last Month', 'Last 3 Months', 'Last 6 Months', 'Last 12 Months', 'Last Year', 'Year to Date', 'All Time']}
/>

```sql revenue
select * from bigquery.monthly_revenue
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
```

```sql pnl
select * from bigquery.monthly_pnl
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
```

```sql retail_breakdown
select
    month,
    revenue_stream,
    revenue
from bigquery.monthly_revenue
where revenue_stream in ('Wholesale', 'Shopify', 'Amazon')
    and month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
```

```sql stream_totals
select
    revenue_stream,
    sum(revenue) as total_revenue,
    sum(transaction_count) as total_transactions,
    avg(revenue) as avg_monthly_revenue
from bigquery.monthly_revenue
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
group by revenue_stream
order by total_revenue desc
```

```sql yoy
select
    extract(month from month) as month_num,
    strftime(month, '%b') as month_name,
    extract(year from month) as year,
    sum(revenue) as revenue
from bigquery.monthly_revenue
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
group by 1, 2, 3
order by month_num
```

```sql revenue_kpi_comparison
select
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        then revenue else 0 end) as total_revenue,
    sum(case when month >= ('${inputs.date_range.start}'::date - interval '12 months')
        and month < '${inputs.date_range.start}'
        then revenue else 0 end) as prior_total_revenue,
    avg(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        then revenue end) as avg_monthly_revenue,
    avg(case when month >= ('${inputs.date_range.start}'::date - interval '12 months')
        and month < '${inputs.date_range.start}'
        then revenue end) as prior_avg_monthly_revenue
from bigquery.monthly_revenue
where month >= ('${inputs.date_range.start}'::date - interval '12 months')
    and month <= '${inputs.date_range.end}'
```

```sql top_stream
select
    revenue_stream,
    sum(revenue) as total_revenue
from bigquery.monthly_revenue
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
group by revenue_stream
order by total_revenue desc
limit 1
```

```sql peak_month
select
    month,
    sum(revenue) as total_revenue
from bigquery.monthly_revenue
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
group by month
order by total_revenue desc
limit 1
```

<BigValue
    data={revenue_kpi_comparison}
    value=total_revenue
    comparison=prior_total_revenue
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    comparisonFmt=usd
    title="Total Revenue"
    fmt=usd
/>

<BigValue
    data={revenue_kpi_comparison}
    value=avg_monthly_revenue
    comparison=prior_avg_monthly_revenue
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    comparisonFmt=usd
    title="Avg Monthly Revenue"
    fmt=usd
/>

<BigValue
    data={top_stream}
    value=revenue_stream
    title="Top Stream"
/>

<BigValue
    data={peak_month}
    value=total_revenue
    title="Peak Month Revenue"
    fmt=usd
/>

## Revenue by Stream (Stacked Area)

<AreaChart
    data={revenue}
    x=month
    y=revenue
    series=revenue_stream
    type=stacked
    title="Monthly Revenue by Stream"
    yFmt=usd
/>

## Retail Breakdown: Shopify vs Amazon vs Wholesale

<BarChart
    data={retail_breakdown}
    x=month
    y=revenue
    series=revenue_stream
    type=grouped
    title="Retail Revenue Breakdown"
    yFmt=usd
/>

## Revenue Stream Summary

<DataTable data={stream_totals}>
    <Column id=revenue_stream title="Stream"/>
    <Column id=total_revenue fmt=usd title="Total Revenue"/>
    <Column id=total_transactions title="Transactions"/>
    <Column id=avg_monthly_revenue fmt=usd title="Avg Monthly"/>
</DataTable>

## Year-over-Year Comparison

<LineChart
    data={yoy}
    x=month_name
    y=revenue
    series=year
    title="Revenue by Month (YoY)"
    yFmt=usd
/>

## Monthly Revenue Detail

<DataTable data={pnl} rows=24>
    <Column id=month fmt=mmm-yy/>
    <Column id=wholesale_revenue fmt=usd title="Wholesale"/>
    <Column id=shopify_revenue fmt=usd title="Shopify"/>
    <Column id=amazon_revenue fmt=usd title="Amazon"/>
    <Column id=affiliate_revenue fmt=usd title="Affiliate"/>
    <Column id=sponsorship_revenue fmt=usd title="Sponsorship"/>
    <Column id=total_revenue fmt=usd title="Total"/>
</DataTable>

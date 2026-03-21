---
title: Financial Overview
---

<DateRange
    name=date_range
    defaultValue="Last 12 Months"
    presetRanges={['Last Month', 'Last 3 Months', 'Last 6 Months', 'Last 12 Months', 'Last Year', 'Year to Date', 'All Time']}
/>

```sql pnl
select * from bigquery.monthly_pnl
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
```

```sql revenue
select * from bigquery.monthly_revenue
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
```

```sql kpi_with_comparison
select
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        then total_revenue else 0 end) as ytd_revenue,
    sum(case when month >= ('${inputs.date_range.start}'::date - interval '12 months')
        and month < '${inputs.date_range.start}'
        then total_revenue else 0 end) as prior_revenue,
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        then operating_expenses else 0 end) as ytd_expenses,
    sum(case when month >= ('${inputs.date_range.start}'::date - interval '12 months')
        and month < '${inputs.date_range.start}'
        then operating_expenses else 0 end) as prior_expenses,
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        then operating_income else 0 end) as ytd_net_income,
    sum(case when month >= ('${inputs.date_range.start}'::date - interval '12 months')
        and month < '${inputs.date_range.start}'
        then operating_income else 0 end) as prior_net_income,
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        then owner_compensation else 0 end) as ytd_owner_comp
from bigquery.monthly_pnl
where month >= ('${inputs.date_range.start}'::date - interval '12 months')
    and month <= '${inputs.date_range.end}'
```

```sql current_month
select
    total_revenue,
    operating_expenses,
    operating_income,
    operating_margin,
    owner_compensation
from bigquery.monthly_pnl
where month = date_trunc('month', current_date)
```

```sql ytd_revenue_mix
select
    revenue_stream,
    sum(revenue) as total_revenue
from bigquery.monthly_revenue
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
group by revenue_stream
order by total_revenue desc
```

<BigValue
    data={kpi_with_comparison}
    value=ytd_revenue
    comparison=prior_revenue
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    comparisonFmt=usd
    title="Total Revenue"
    fmt=usd
/>

<BigValue
    data={kpi_with_comparison}
    value=ytd_expenses
    comparison=prior_expenses
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    comparisonFmt=usd
    downIsGood={true}
    title="Operating Expenses"
    fmt=usd
/>

<BigValue
    data={kpi_with_comparison}
    value=ytd_net_income
    comparison=prior_net_income
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    comparisonFmt=usd
    title="Operating Income"
    fmt=usd
/>

<BigValue
    data={kpi_with_comparison}
    value=ytd_owner_comp
    title="Owner Compensation"
    fmt=usd
    downIsGood={true}
/>

## Monthly Revenue by Stream

<BarChart
    data={revenue}
    x=month
    y=revenue
    series=revenue_stream
    type=stacked
    title="Monthly Revenue by Stream"
    yFmt=usd
/>

## Operating Income Trend

<LineChart
    data={pnl}
    x=month
    y=operating_income
    title="Monthly Operating Income (excl. owner compensation)"
    yFmt=usd
/>

## Revenue Mix

<ECharts config={
    {
        tooltip: { trigger: 'item' },
        series: [
            {
                type: 'pie',
                radius: ['40%', '70%'],
                data: ytd_revenue_mix.map(row => ({
                    name: row.revenue_stream,
                    value: row.total_revenue
                }))
            }
        ]
    }
}/>

## Monthly P&L Detail

<DataTable data={pnl} rows=24>
    <Column id=month fmt=mmm-yy/>
    <Column id=total_revenue fmt=usd title="Revenue"/>
    <Column id=retail_revenue fmt=usd title="Retail"/>
    <Column id=affiliate_revenue fmt=usd title="Affiliate"/>
    <Column id=sponsorship_revenue fmt=usd title="Sponsorship"/>
    <Column id=operating_expenses fmt=usd title="Op. Expenses"/>
    <Column id=owner_compensation fmt=usd title="Owner Comp"/>
    <Column id=operating_income fmt=usd title="Op. Income"/>
    <Column id=operating_margin fmt=pct1 title="Op. Margin"/>
</DataTable>

---
title: Affiliate Partners
---

<DateRange
    name=date_range
    defaultValue="Last 12 Months"
    presetRanges={['Last Month', 'Last 3 Months', 'Last 6 Months', 'Last 12 Months', 'Last Year', 'Year to Date', 'All Time']}
/>

```sql affiliate_data
select * from bigquery.affiliate_by_partner
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
```

```sql partner_totals
select
    partner_name,
    sum(revenue) as total_revenue,
    sum(transaction_count) as total_transactions,
    count(distinct month) as months_active,
    sum(revenue) / count(distinct month) as avg_monthly_revenue,
    min(month) as first_month,
    max(month) as last_month
from bigquery.affiliate_by_partner
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
group by partner_name
order by total_revenue desc
```

```sql monthly_by_partner
select
    month,
    partner_name,
    sum(revenue) as revenue
from bigquery.affiliate_by_partner
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
group by 1, 2
order by month, partner_name
```

```sql top_partners_monthly
select
    month,
    partner_name,
    sum(revenue) as revenue
from bigquery.affiliate_by_partner
where partner_name not in ('Unidentified', 'Unknown (Mobile Deposit)')
    and month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
group by 1, 2
order by month
```

```sql monthly_totals
select
    month,
    sum(revenue) as total_revenue,
    sum(transaction_count) as total_transactions,
    count(distinct partner_name) as active_partners
from bigquery.affiliate_by_partner
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
group by 1
order by month
```

```sql channel_split
select
    payment_channel,
    sum(revenue) as total_revenue,
    sum(transaction_count) as total_transactions
from bigquery.affiliate_by_partner
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
group by 1
order by total_revenue desc
```

```sql affiliate_kpi_comparison
select
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        then revenue else 0 end) as ytd_revenue,
    sum(case when month >= ('${inputs.date_range.start}'::date - interval '12 months')
        and month < '${inputs.date_range.start}'
        then revenue else 0 end) as prior_revenue,
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        and partner_name not in ('Unidentified', 'Unknown (Mobile Deposit)')
        then revenue else 0 end) as identified_revenue,
    sum(case when month >= ('${inputs.date_range.start}'::date - interval '12 months')
        and month < '${inputs.date_range.start}'
        and partner_name not in ('Unidentified', 'Unknown (Mobile Deposit)')
        then revenue else 0 end) as prior_identified_revenue,
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        and partner_name in ('Unidentified', 'Unknown (Mobile Deposit)')
        then revenue else 0 end) as unidentified_revenue,
    count(distinct case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        and partner_name not in ('Unidentified', 'Unknown (Mobile Deposit)')
        then partner_name end) as identified_partners
from bigquery.affiliate_by_partner
where month >= ('${inputs.date_range.start}'::date - interval '12 months')
    and month <= '${inputs.date_range.end}'
```

<BigValue
    data={affiliate_kpi_comparison}
    value=ytd_revenue
    comparison=prior_revenue
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    comparisonFmt=usd
    fmt=usd
    title="Total Affiliate Revenue"
/>

<BigValue
    data={affiliate_kpi_comparison}
    value=identified_revenue
    comparison=prior_identified_revenue
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    comparisonFmt=usd
    fmt=usd
    title="Identified Partner Revenue"
/>

<BigValue
    data={affiliate_kpi_comparison}
    value=unidentified_revenue
    fmt=usd
    title="Unidentified Revenue"
/>

<BigValue
    data={affiliate_kpi_comparison}
    value=identified_partners
    title="Active Partners"
/>

## Monthly Affiliate Revenue Trend

<BarChart
    data={monthly_totals}
    x=month
    y=total_revenue
    title="Total Affiliate Revenue by Month"
    yFmt=usd
/>

## Revenue by Partner (Top Partners)

<AreaChart
    data={top_partners_monthly}
    x=month
    y=revenue
    series=partner_name
    type=stacked
    title="Monthly Revenue by Partner"
    yFmt=usd
/>

## Partner Summary

<DataTable data={partner_totals} rows=30>
    <Column id=partner_name title="Partner"/>
    <Column id=total_revenue fmt=usd title="Total Revenue"/>
    <Column id=total_transactions title="Transactions"/>
    <Column id=months_active title="Months Active"/>
    <Column id=avg_monthly_revenue fmt=usd title="Avg Monthly"/>
    <Column id=first_month fmt=mmm-yy title="First"/>
    <Column id=last_month fmt=mmm-yy title="Last"/>
</DataTable>

## Payment Channel Split

<BarChart
    data={channel_split}
    x=payment_channel
    y=total_revenue
    title="Revenue by Payment Channel"
    yFmt=usd
/>

## Monthly Detail by Partner

<DataTable data={monthly_by_partner} rows=50>
    <Column id=month fmt=mmm-yy/>
    <Column id=partner_name title="Partner"/>
    <Column id=revenue fmt=usd title="Revenue"/>
</DataTable>

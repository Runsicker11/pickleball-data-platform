---
title: D2C Profitability
---

Shopify direct shop (pickleballeffectshop.com). Amazon excluded — treated as a separate business.

**Contribution stack**: net revenue − COGS − shipping ($5.35/order) − Shopify Payments (2.9% + $0.30) − Google ad spend − Meta ad spend. No operating overhead allocated.

<DateRange
    name=date_range
    defaultValue="Last 12 Months"
    presetRanges={['Last Month', 'Last 3 Months', 'Last 6 Months', 'Last 12 Months', 'Year to Date', 'All Time']}
/>

```sql pnl
select * from bigquery.d2c_monthly_pnl
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
order by month
```

```sql kpi
select
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        then net_revenue else 0 end) as period_revenue,
    sum(case when month >= ('${inputs.date_range.start}'::date - interval '12 months')
        and month < '${inputs.date_range.start}'
        then net_revenue else 0 end) as prior_revenue,
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        then d2c_contribution else 0 end) as period_contribution,
    sum(case when month >= ('${inputs.date_range.start}'::date - interval '12 months')
        and month < '${inputs.date_range.start}'
        then d2c_contribution else 0 end) as prior_contribution,
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        then total_ad_spend else 0 end) as period_ad_spend,
    sum(case when month >= ('${inputs.date_range.start}'::date - interval '12 months')
        and month < '${inputs.date_range.start}'
        then total_ad_spend else 0 end) as prior_ad_spend,
    sum(case when month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
        then orders else 0 end) as period_orders
from bigquery.d2c_monthly_pnl
where month >= ('${inputs.date_range.start}'::date - interval '12 months')
    and month <= '${inputs.date_range.end}'
```

```sql contribution_breakdown
select
    month,
    net_revenue,
    total_cogs * -1 as cogs,
    estimated_shipping * -1 as shipping,
    shopify_fees_est * -1 as shopify_fees,
    google_ads_spend * -1 as google,
    meta_ads_spend * -1 as meta,
    d2c_contribution
from bigquery.d2c_monthly_pnl
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
order by month
```

```sql cost_stack_long
select month, 'COGS' as line, total_cogs as value from bigquery.d2c_monthly_pnl
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
union all
select month, 'Shipping', estimated_shipping from bigquery.d2c_monthly_pnl
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
union all
select month, 'Shopify Fees', shopify_fees_est from bigquery.d2c_monthly_pnl
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
union all
select month, 'Google Ads', google_ads_spend from bigquery.d2c_monthly_pnl
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
union all
select month, 'Meta Ads', meta_ads_spend from bigquery.d2c_monthly_pnl
where month >= '${inputs.date_range.start}' and month <= '${inputs.date_range.end}'
order by month, line
```

```sql biweekly
select
    date_trunc(order_date, week(monday)) +
        interval '7' day * mod(extract(week from order_date), 2) as period_start,
    sum(orders) as orders,
    sum(net_revenue) as net_revenue,
    sum(total_cogs) as total_cogs,
    sum(estimated_shipping) as estimated_shipping,
    sum(shopify_fees_est) as shopify_fees_est,
    sum(google_ads_spend) as google_ads_spend,
    sum(meta_ads_spend) as meta_ads_spend,
    sum(d2c_contribution) as d2c_contribution,
    safe_divide(sum(d2c_contribution), sum(net_revenue)) as margin
from `practical-gecko-373320`.int_marketing.int_marketing__d2c_daily_pnl
where order_date >= '${inputs.date_range.start}' and order_date <= '${inputs.date_range.end}'
group by period_start
order by period_start
```

<BigValue
    data={kpi}
    value=period_revenue
    comparison=prior_revenue
    comparisonTitle="vs Prior 12 mo"
    comparisonDelta={true}
    comparisonFmt=usd
    title="D2C Net Revenue"
    fmt=usd
/>

<BigValue
    data={kpi}
    value=period_contribution
    comparison=prior_contribution
    comparisonTitle="vs Prior 12 mo"
    comparisonDelta={true}
    comparisonFmt=usd
    title="D2C Contribution"
    fmt=usd
/>

<BigValue
    data={kpi}
    value=period_ad_spend
    comparison=prior_ad_spend
    comparisonTitle="vs Prior 12 mo"
    comparisonDelta={true}
    comparisonFmt=usd
    downIsGood={true}
    title="Ad Spend"
    fmt=usd
/>

<BigValue
    data={kpi}
    value=period_orders
    title="Orders"
    fmt=num0
/>

## Monthly Contribution

<LineChart
    data={pnl}
    x=month
    y=d2c_contribution
    title="D2C Contribution (net of COGS, shipping, fees, ads)"
    yFmt=usd
/>

## Revenue vs Costs

<BarChart
    data={cost_stack_long}
    x=month
    y=value
    series=line
    type=stacked
    title="Monthly Cost Stack ($)"
    yFmt=usd
/>

## Bi-weekly Trend

<DataTable data={biweekly} rows=15>
    <Column id=period_start fmt=mmm-dd title="2-Week Start"/>
    <Column id=orders title="Orders"/>
    <Column id=net_revenue fmt=usd title="Net Rev"/>
    <Column id=total_cogs fmt=usd title="COGS"/>
    <Column id=google_ads_spend fmt=usd title="Google"/>
    <Column id=meta_ads_spend fmt=usd title="Meta"/>
    <Column id=d2c_contribution fmt=usd title="Contribution"/>
    <Column id=margin fmt=pct1 title="Margin"/>
</DataTable>

## Monthly P&L Detail

<DataTable data={pnl} rows=18>
    <Column id=month fmt=mmm-yy title="Month"/>
    <Column id=orders title="Orders"/>
    <Column id=net_revenue fmt=usd title="Net Rev"/>
    <Column id=total_cogs fmt=usd title="COGS"/>
    <Column id=estimated_shipping fmt=usd title="Shipping"/>
    <Column id=shopify_fees_est fmt=usd title="Shopify Fees"/>
    <Column id=google_ads_spend fmt=usd title="Google"/>
    <Column id=meta_ads_spend fmt=usd title="Meta"/>
    <Column id=d2c_contribution fmt=usd title="Contribution"/>
    <Column id=d2c_contribution_margin fmt=pct1 title="Margin"/>
    <Column id=blended_roas fmt=num1 title="Blended ROAS"/>
</DataTable>

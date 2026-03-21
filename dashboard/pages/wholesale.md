---
title: Wholesale
---

<DateRange
    name=date_range
    defaultValue="Last 12 Months"
    presetRanges={['Last Month', 'Last 3 Months', 'Last 6 Months', 'Last 12 Months', 'Last Year', 'Year to Date', 'All Time']}
/>

```sql wholesale
select * from bigquery.wholesale_detail
where txn_date >= '${inputs.date_range.start}' and txn_date <= '${inputs.date_range.end}'
```

```sql revenue_by_customer
select
    customer_name,
    coalesce(company_name, customer_name) as company,
    state,
    sum(amount) as total_revenue,
    count(distinct invoice_id) as invoice_count,
    sum(quantity) as total_units
from bigquery.wholesale_detail
where txn_date >= '${inputs.date_range.start}' and txn_date <= '${inputs.date_range.end}'
group by 1, 2, 3
order by total_revenue desc
```

```sql top_products
select
    item_name,
    sum(quantity) as total_quantity,
    sum(amount) as total_revenue,
    avg(unit_price) as avg_price
from bigquery.wholesale_detail
where item_name is not null
    and txn_date >= '${inputs.date_range.start}' and txn_date <= '${inputs.date_range.end}'
group by item_name
order by total_revenue desc
limit 20
```

```sql monthly_trend
select
    date_trunc('month', txn_date) as month,
    sum(amount) as revenue,
    count(distinct invoice_id) as invoice_count,
    count(distinct customer_name) as customer_count
from bigquery.wholesale_detail
where txn_date >= '${inputs.date_range.start}' and txn_date <= '${inputs.date_range.end}'
group by 1
order by 1
```

```sql outstanding
select
    invoice_id,
    txn_date,
    customer_name,
    company_name,
    invoice_total,
    invoice_balance,
    txn_status
from bigquery.wholesale_detail
where invoice_balance > 0
    and txn_date >= '${inputs.date_range.start}' and txn_date <= '${inputs.date_range.end}'
group by 1, 2, 3, 4, 5, 6, 7
order by invoice_balance desc
```

```sql wholesale_kpi_comparison
select
    sum(case when txn_date >= '${inputs.date_range.start}' and txn_date <= '${inputs.date_range.end}'
        then amount else 0 end) as total_revenue,
    sum(case when txn_date >= ('${inputs.date_range.start}'::date - interval '12 months')
        and txn_date < '${inputs.date_range.start}'
        then amount else 0 end) as prior_total_revenue,
    count(distinct case when txn_date >= '${inputs.date_range.start}' and txn_date <= '${inputs.date_range.end}'
        then invoice_id end) as invoice_count,
    count(distinct case when txn_date >= ('${inputs.date_range.start}'::date - interval '12 months')
        and txn_date < '${inputs.date_range.start}'
        then invoice_id end) as prior_invoice_count,
    count(distinct case when txn_date >= '${inputs.date_range.start}' and txn_date <= '${inputs.date_range.end}'
        then customer_name end) as active_customers
from bigquery.wholesale_detail
where txn_date >= ('${inputs.date_range.start}'::date - interval '12 months')
    and txn_date <= '${inputs.date_range.end}'
```

```sql outstanding_total
select
    sum(invoice_balance) as outstanding_balance
from bigquery.wholesale_detail
where invoice_balance > 0
    and txn_date >= '${inputs.date_range.start}' and txn_date <= '${inputs.date_range.end}'
```

<BigValue
    data={wholesale_kpi_comparison}
    value=total_revenue
    comparison=prior_total_revenue
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    comparisonFmt=usd
    title="Total Wholesale Revenue"
    fmt=usd
/>

<BigValue
    data={wholesale_kpi_comparison}
    value=invoice_count
    comparison=prior_invoice_count
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    title="Invoices"
/>

<BigValue
    data={wholesale_kpi_comparison}
    value=active_customers
    title="Active Customers"
/>

<BigValue
    data={outstanding_total}
    value=outstanding_balance
    title="Outstanding Balance"
    fmt=usd
/>

## Monthly Wholesale Trend

<BarChart
    data={monthly_trend}
    x=month
    y=revenue
    title="Monthly Wholesale Revenue"
    yFmt=usd
/>

## Revenue by Customer

<DataTable data={revenue_by_customer}>
    <Column id=company title="Company"/>
    <Column id=state title="State"/>
    <Column id=total_revenue fmt=usd title="Revenue"/>
    <Column id=invoice_count title="Invoices"/>
    <Column id=total_units title="Units"/>
</DataTable>

## Top Wholesale Products

<BarChart
    data={top_products}
    x=item_name
    y=total_revenue
    title="Top Products by Revenue"
    yFmt=usd
    swapXY=true
/>

<DataTable data={top_products}>
    <Column id=item_name title="Product"/>
    <Column id=total_quantity title="Qty Sold"/>
    <Column id=total_revenue fmt=usd title="Revenue"/>
    <Column id=avg_price fmt=usd title="Avg Price"/>
</DataTable>

## Outstanding Invoices

<DataTable data={outstanding}>
    <Column id=invoice_id title="Invoice"/>
    <Column id=txn_date fmt=date title="Date"/>
    <Column id=customer_name title="Customer"/>
    <Column id=invoice_total fmt=usd title="Total"/>
    <Column id=invoice_balance fmt=usd title="Balance"/>
    <Column id=txn_status title="Status"/>
</DataTable>

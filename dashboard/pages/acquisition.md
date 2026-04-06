---
title: Acquisition
order: 3
---

<DateRange
    name=date_range
    defaultValue="Last 30 Days"
    presetRanges={['Last 7 Days', 'Last 30 Days', 'Last 3 Months', 'Last 6 Months', 'Last 12 Months', 'All Time']}
/>

```sql kpi_current
select
    sum(case when channel in ('google_ads', 'meta') then spend else 0 end) as total_spend,
    sum(case when channel in ('google_ads', 'meta') then revenue else 0 end) /
        nullif(sum(case when channel in ('google_ads', 'meta') then spend else 0 end), 0) as blended_roas,
    sum(case when channel = 'google_ads' then revenue else 0 end) /
        nullif(sum(case when channel = 'google_ads' then spend else 0 end), 0) as google_roas,
    sum(case when channel = 'meta' then revenue else 0 end) /
        nullif(sum(case when channel = 'meta' then spend else 0 end), 0) as meta_roas
from bigquery.paid_channel_daily
where report_date >= '${inputs.date_range.start}'::date
  and report_date <= '${inputs.date_range.end}'::date
```

```sql kpi_prior
select
    sum(case when channel in ('google_ads', 'meta') then spend else 0 end) as prior_total_spend,
    sum(case when channel in ('google_ads', 'meta') then revenue else 0 end) /
        nullif(sum(case when channel in ('google_ads', 'meta') then spend else 0 end), 0) as prior_blended_roas,
    sum(case when channel = 'google_ads' then revenue else 0 end) /
        nullif(sum(case when channel = 'google_ads' then spend else 0 end), 0) as prior_google_roas,
    sum(case when channel = 'meta' then revenue else 0 end) /
        nullif(sum(case when channel = 'meta' then spend else 0 end), 0) as prior_meta_roas
from bigquery.paid_channel_daily
where report_date >= ('${inputs.date_range.start}'::date - ('${inputs.date_range.end}'::date - '${inputs.date_range.start}'::date + interval '1 day'))
  and report_date < '${inputs.date_range.start}'::date
```

```sql weekly_spend
select
    date_trunc('week', report_date) as week,
    channel,
    sum(spend) as spend
from bigquery.paid_channel_daily
where report_date >= '${inputs.date_range.start}'::date
  and report_date <= '${inputs.date_range.end}'::date
  and channel in ('google_ads', 'meta')
group by 1, 2
order by week, channel
```

```sql weekly_roas
select
    date_trunc('week', report_date) as week,
    channel,
    sum(revenue) / nullif(sum(spend), 0) as roas
from bigquery.paid_channel_daily
where report_date >= '${inputs.date_range.start}'::date
  and report_date <= '${inputs.date_range.end}'::date
  and channel in ('google_ads', 'meta')
group by 1, 2
order by week, channel
```

```sql google_campaigns
select
    campaign_name,
    sum(spend) as spend,
    sum(clicks) as clicks,
    sum(conversions) as conversions,
    sum(conversion_value) as revenue,
    sum(conversion_value) / nullif(sum(spend), 0) as roas,
    sum(spend) / nullif(sum(clicks), 0) as cpc,
    sum(conversions) / nullif(sum(clicks), 0) as conversion_rate
from bigquery.paid_campaign_daily
where report_date >= '${inputs.date_range.start}'::date
  and report_date <= '${inputs.date_range.end}'::date
  and platform = 'google_ads'
group by campaign_name
order by spend desc
```

```sql meta_campaigns
select
    campaign_name,
    sum(spend) as spend,
    sum(clicks) as clicks,
    sum(conversions) as conversions,
    sum(conversion_value) as revenue,
    sum(conversion_value) / nullif(sum(spend), 0) as roas,
    sum(spend) / nullif(sum(clicks), 0) as cpc,
    sum(conversions) / nullif(sum(clicks), 0) as conversion_rate
from bigquery.paid_campaign_daily
where report_date >= '${inputs.date_range.start}'::date
  and report_date <= '${inputs.date_range.end}'::date
  and platform = 'meta'
group by campaign_name
order by spend desc
```

<BigValue
    data={kpi_current}
    value=total_spend
    comparison={kpi_prior[0].prior_total_spend}
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    comparisonFmt=usd
    downIsGood={true}
    title="Total Ad Spend"
    fmt=usd
/>

<BigValue
    data={kpi_current}
    value=blended_roas
    comparison={kpi_prior[0].prior_blended_roas}
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    title="Blended ROAS"
    fmt=num2
/>

<BigValue
    data={kpi_current}
    value=google_roas
    comparison={kpi_prior[0].prior_google_roas}
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    title="Google ROAS"
    fmt=num2
/>

<BigValue
    data={kpi_current}
    value=meta_roas
    comparison={kpi_prior[0].prior_meta_roas}
    comparisonTitle="vs Prior Period"
    comparisonDelta={true}
    title="Meta ROAS"
    fmt=num2
/>

> **Attribution note:** ROAS is GA4-attributed — revenue is matched to GA4 purchase events joined to Shopify orders. Google Ads UTM population is incomplete (fix pending with agency); direct/unknown traffic may contain paid visits.

## Weekly Spend by Channel

<BarChart
    data={weekly_spend}
    x=week
    y=spend
    series=channel
    type=grouped
    title="Weekly Ad Spend: Google vs Meta"
    yFmt=usd
/>

## Weekly ROAS by Channel

<LineChart
    data={weekly_roas}
    x=week
    y=roas
    series=channel
    title="Weekly ROAS by Channel (GA4-attributed)"
    referenceLine={1.8}
    referenceLineLabel="Floor (1.8x)"
    referenceLine={3.0}
    referenceLineLabel="Target (3.0x)"
/>

## Google Ads: Campaign Performance

<DataTable data={google_campaigns} rows=20>
    <Column id=campaign_name title="Campaign"/>
    <Column id=spend fmt=usd title="Spend"/>
    <Column id=clicks fmt=num0 title="Clicks"/>
    <Column id=conversions fmt=num0 title="Conversions"/>
    <Column id=revenue fmt=usd title="Revenue"/>
    <Column id=roas fmt=num2 title="ROAS"/>
    <Column id=cpc fmt=usd title="CPC"/>
    <Column id=conversion_rate fmt=pct1 title="Conv. Rate"/>
</DataTable>

## Meta Ads: Campaign Performance

<DataTable data={meta_campaigns} rows=20>
    <Column id=campaign_name title="Campaign"/>
    <Column id=spend fmt=usd title="Spend"/>
    <Column id=clicks fmt=num0 title="Clicks"/>
    <Column id=conversions fmt=num0 title="Conversions"/>
    <Column id=revenue fmt=usd title="Revenue"/>
    <Column id=roas fmt=num2 title="ROAS"/>
    <Column id=cpc fmt=usd title="CPC"/>
    <Column id=conversion_rate fmt=pct1 title="Conv. Rate"/>
</DataTable>

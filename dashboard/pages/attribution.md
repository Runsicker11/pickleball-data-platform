---
title: Attribution
order: 4
---

<DateRange
    name=date_range
    defaultValue="Last 30 Days"
    presetRanges={['Last 7 Days', 'Last 30 Days', 'Last 3 Months', 'Last 6 Months', 'Last 12 Months', 'All Time']}
/>

```sql survey_kpi
select
    count(*) as total_responses,
    sum(case when how_did_you_hear like 'Search Engine%' then 1 else 0 end) * 1.0 / count(*) as pct_search,
    sum(case when how_did_you_hear like 'Pickleball Effect Podcast%'
              or how_did_you_hear like 'From an Influencer%' then 1 else 0 end) * 1.0 / count(*) as pct_podcast_influencer,
    sum(case when how_did_you_hear = 'Friend or Family'
              or how_did_you_hear like '%other%' then 1 else 0 end) * 1.0 / count(*) as pct_wom_other
from bigquery.grapevine_survey
where responded_at::date >= '${inputs.date_range.start}'::date
  and responded_at::date <= '${inputs.date_range.end}'::date
```

```sql survey_mix
select
    case
      when how_did_you_hear like 'Search Engine%' then 'Search Engine'
      when how_did_you_hear like 'Social Media%' then 'Social Media'
      when how_did_you_hear like 'Pickleball Effect Podcast%' then 'PE Podcast'
      when how_did_you_hear like 'From an Influencer%' then 'Influencer'
      when how_did_you_hear = 'Friend or Family' then 'Word of Mouth'
      when how_did_you_hear like '%other%' then 'Other Community'
      else 'Other'
    end as channel_category,
    count(*) as responses
from bigquery.grapevine_survey
where responded_at::date >= '${inputs.date_range.start}'::date
  and responded_at::date <= '${inputs.date_range.end}'::date
group by 1
order by responses desc
```

```sql survey_over_time
select
    date_trunc('week', responded_at::date) as week,
    case
      when how_did_you_hear like 'Search Engine%' then 'Search Engine'
      when how_did_you_hear like 'Social Media%' then 'Social Media'
      when how_did_you_hear like 'Pickleball Effect Podcast%' then 'PE Podcast'
      when how_did_you_hear like 'From an Influencer%' then 'Influencer'
      when how_did_you_hear = 'Friend or Family' then 'Word of Mouth'
      when how_did_you_hear like '%other%' then 'Other Community'
      else 'Other'
    end as channel_category,
    count(*) as response_count
from bigquery.grapevine_survey
where responded_at::date >= '${inputs.date_range.start}'::date
  and responded_at::date <= '${inputs.date_range.end}'::date
group by 1, 2
order by week, channel_category
```

```sql survey_aov
select
    case
      when how_did_you_hear like 'Search Engine%' then 'Search Engine'
      when how_did_you_hear like 'Social Media%' then 'Social Media'
      when how_did_you_hear like 'Pickleball Effect Podcast%' then 'PE Podcast'
      when how_did_you_hear like 'From an Influencer%' then 'Influencer'
      when how_did_you_hear = 'Friend or Family' then 'Word of Mouth'
      when how_did_you_hear like '%other%' then 'Other Community'
      else 'Other'
    end as channel_category,
    count(*) as responses,
    avg(order_total) as avg_order_value,
    sum(order_total) as total_revenue
from bigquery.grapevine_survey
where responded_at::date >= '${inputs.date_range.start}'::date
  and responded_at::date <= '${inputs.date_range.end}'::date
group by 1
order by responses desc
```

```sql ga4_channel_orders
select
    channel,
    sum(orders) as attributed_orders,
    sum(revenue) as attributed_revenue
from bigquery.paid_channel_daily
where report_date >= '${inputs.date_range.start}'::date
  and report_date <= '${inputs.date_range.end}'::date
group by 1
order by attributed_orders desc
```

```sql channel_comparison
with survey as (
    select
        case
          when how_did_you_hear like 'Search Engine%' then 'Search Engine'
          when how_did_you_hear like 'Social Media%' then 'Social Media'
          when how_did_you_hear like 'Pickleball Effect Podcast%' then 'PE Podcast'
          when how_did_you_hear like 'From an Influencer%' then 'Influencer'
          when how_did_you_hear = 'Friend or Family' then 'Word of Mouth'
          when how_did_you_hear like '%other%' then 'Other Community'
          else 'Other'
        end as channel,
        count(*) as survey_count,
        avg(order_total) as avg_order_value
    from bigquery.grapevine_survey
    where responded_at::date >= '${inputs.date_range.start}'::date
      and responded_at::date <= '${inputs.date_range.end}'::date
    group by 1
),
survey_total as (select sum(survey_count) as t from survey)
select
    s.channel,
    s.survey_count as survey_responses,
    round(s.survey_count * 100.0 / st.t, 1) as survey_pct,
    s.avg_order_value
from survey s
cross join survey_total st
order by s.survey_count desc
```

<BigValue
    data={survey_kpi}
    value=total_responses
    title="Survey Responses"
    fmt=num0
/>

<BigValue
    data={survey_kpi}
    value=pct_search
    title="Via Search Engine"
    fmt=pct1
/>

<BigValue
    data={survey_kpi}
    value=pct_podcast_influencer
    title="Podcast / Influencer"
    fmt=pct1
/>

<BigValue
    data={survey_kpi}
    value=pct_wom_other
    title="Word of Mouth / Other"
    fmt=pct1
/>

> **Data limitations:** Grapevine survey is post-purchase self-report — responses lag purchases by hours/days. GA4 attribution uses session source at time of purchase event. Direct/unknown in GA4 likely contains paid and organic visits obscured by cross-domain tracking gaps.

## Self-Reported Attribution Mix

<ECharts config={
    {
        tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
        series: [
            {
                type: 'pie',
                radius: ['40%', '70%'],
                data: survey_mix.map(row => ({
                    name: row.channel_category,
                    value: row.responses
                }))
            }
        ]
    }
}/>

## Survey Responses Over Time

<BarChart
    data={survey_over_time}
    x=week
    y=response_count
    series=channel_category
    type=stacked
    title="Weekly Survey Responses by Channel"
/>

## Average Order Value by Attribution Channel

<BarChart
    data={survey_aov}
    x=channel_category
    y=avg_order_value
    title="Avg Order Value by Self-Reported Channel"
    yFmt=usd
    swapXY={true}
/>

## GA4-Attributed Orders by Channel

<ECharts config={
    {
        tooltip: { trigger: 'item', formatter: '{b}: {c} orders ({d}%)' },
        series: [
            {
                type: 'pie',
                radius: ['40%', '70%'],
                data: ga4_channel_orders.map(row => ({
                    name: row.channel,
                    value: row.attributed_orders
                }))
            }
        ]
    }
}/>

## Self-Reported Channel Detail

<DataTable data={channel_comparison}>
    <Column id=channel title="Channel (Survey)"/>
    <Column id=survey_responses fmt=num0 title="Responses"/>
    <Column id=survey_pct fmt=num1 title="% of Surveys"/>
    <Column id=avg_order_value fmt=usd title="Avg Order Value"/>
</DataTable>

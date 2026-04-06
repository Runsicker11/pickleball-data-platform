---
title: Pipeline Health
order: 9
---

```sql daily_failures
select
    run_date,
    count(*) filter (where status = 'error' and pipeline_name != 'dbt') as failures,
    count(*) filter (where status = 'success' and pipeline_name != 'dbt') as successes
from pipeline_runs
where run_date >= current_date - interval '30 days'
group by run_date
order by run_date
```

```sql pipeline_rates
select
    pipeline_name,
    count(*) as total_runs,
    count(*) filter (where status = 'success') as successes,
    count(*) filter (where status = 'error') as failures,
    round(100.0 * count(*) filter (where status = 'success') / count(*), 1) as success_rate
from pipeline_runs
where run_date >= current_date - interval '30 days'
  and pipeline_name != 'dbt'
group by pipeline_name
order by success_rate asc, failures desc
```

```sql recent_failures
select
    started_at::timestamp as started_at,
    pipeline_name,
    round(duration_seconds, 0) as duration_seconds,
    error_message
from pipeline_runs
where status = 'error'
order by started_at desc
limit 25
```

```sql dbt_health
select
    run_date,
    status,
    round(duration_seconds, 0) as duration_seconds,
    error_message
from pipeline_runs
where pipeline_name = 'dbt'
order by run_date desc
limit 30
```

# Pipeline Health

## Daily Failures (Last 30 Days)

<BarChart
    data={daily_failures}
    x=run_date
    y=failures
    yAxisTitle="Failed Runs"
    colorPalette={['#ef4444']}
    title="Pipeline Failures per Day"
/>

## 30-Day Success Rates by Pipeline

<DataTable
    data={pipeline_rates}
    rows=20
>
    <Column id=pipeline_name title="Pipeline" />
    <Column id=total_runs title="Runs" align=center />
    <Column id=successes title="✓ Success" align=center />
    <Column id=failures title="✗ Failures" align=center contentType=colorscale colorScale={['#16a34a','#fbbf24','#ef4444']} scaleColor=failures />
    <Column id=success_rate title="Rate" align=center fmt="0.0'%'" />
</DataTable>

## Recent Failures

<DataTable
    data={recent_failures}
    rows=25
>
    <Column id=started_at title="Time" fmt="yyyy-MM-dd HH:mm" />
    <Column id=pipeline_name title="Pipeline" />
    <Column id=duration_seconds title="Duration (s)" align=right />
    <Column id=error_message title="Error" wrap=true />
</DataTable>

## dbt Run History

<DataTable
    data={dbt_health}
    rows=15
>
    <Column id=run_date title="Date" />
    <Column id=status title="Status" contentType=colorscale colorScale={['#16a34a','#ef4444']} />
    <Column id=duration_seconds title="Duration (s)" align=right />
    <Column id=error_message title="Error" wrap=true />
</DataTable>

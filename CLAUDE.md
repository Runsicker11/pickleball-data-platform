# Pickleball Data Platform

## Project Overview
Unified data pipeline for Pickleball Effect using dlt (ingestion) + dbt (transformation) + BigQuery.
GCP Project: `practical-gecko-373320`

## Architecture
- **dlt pipelines** in `pipelines/` — extract from APIs, load to BigQuery `raw_amazon`
- **dbt models** in `dbt_project/` — transform raw → staging → intermediate → marts
- **Terraform** in `terraform/` — Cloud Run Jobs, Scheduler, IAM, BigQuery datasets
- **Docker** in `docker/` — container images for dlt and dbt Cloud Run Jobs

## Key Commands
```bash
# Run Amazon Ads pipeline (local)
python -m pipelines.run amazon-ads --days 7

# Run Amazon Ads pipeline to DuckDB (testing)
python -m pipelines.run amazon-ads --days 7 --destination duckdb

# dbt
cd dbt_project && dbt run && dbt test

# Lint
ruff check pipelines/ tests/

# Test
pytest tests/
```

## BigQuery Datasets
- `raw_amazon` — dlt-loaded raw data (5 tables)
- `stg_amazon` — dbt staging models
- `int_amazon` — dbt intermediate models
- `bi` — dbt mart models (future)

## Environment Variables
See `.env.example`. Credentials in `.env` (gitignored).

## Conventions
- Python 3.11+, type hints on public functions
- dlt resources use `write_disposition="merge"` with explicit primary keys
- dbt models follow: sources → staging (stg_) → intermediate (int_) → marts (fct_/dim_)
- All money values stored as FLOAT64 in raw, NUMERIC in dbt

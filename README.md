# Pickleball Data Platform

Unified data pipeline for **Pickleball Effect** — ingests Amazon Ads (and eventually Shopify, Meta, Google Ads, Search Console) into BigQuery using [dlt](https://dlthub.com/) + [dbt](https://www.getdbt.com/).

## Quick Start

```bash
# Install
pip install -e ".[dev]"

# Copy and fill in credentials
cp .env.example .env

# Run Amazon Ads pipeline (last 7 days, to DuckDB for testing)
python -m pipelines.run amazon-ads --days 7 --destination duckdb

# Run to BigQuery
python -m pipelines.run amazon-ads --days 7
```

## Architecture

```
Amazon Ads API → dlt → BigQuery raw_amazon → dbt → stg/int/marts
```

See `CLAUDE.md` for full details.

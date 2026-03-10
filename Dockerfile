FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (cache layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --extra dbt --extra seller --no-dev

# Copy source code
COPY . .
ENV PYTHONPATH=/app

# dbt profiles.yml — use the existing one from the repo
RUN mkdir -p /root/.dbt
COPY dbt_project/profiles.yml /root/.dbt/profiles.yml

# Use venv Python directly + ensure dbt is on PATH
ENV PATH="/app/.venv/bin:$PATH"

# Secret Manager mounts .env at /secrets/.env — copy it into /app at runtime
CMD ["sh", "-c", "cp /secrets/.env /app/.env 2>/dev/null; python -m pipelines.run_all --days 3 --skip search-console"]

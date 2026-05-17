FROM python:3.12-slim

LABEL org.opencontainers.image.title="Vigil ML Pipeline"
LABEL org.opencontainers.image.description="Medicare fraud ML pipeline: ingest, feature engineering, training, scoring, flag generation"

WORKDIR /ml

# System deps — gcc for pandas/numpy C extensions, libpq-dev for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create pipeline data directories (bind-mount these in production for persistence)
RUN mkdir -p data/raw data/processed models

# Default: score existing parquet + load DB (fast path, no re-download/re-train)
# Override CMD at runtime for other modes:
#   docker run vigil/ml --leie-only
#   docker run vigil/ml --skip-ingest --skip-train
#   docker run vigil/ml  (full pipeline)
ENTRYPOINT ["python", "run_pipeline.py"]
CMD ["--skip-ingest", "--skip-train"]

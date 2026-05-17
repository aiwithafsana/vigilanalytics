FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for asyncpg + psycopg2 (entrypoint DB wait)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Make entrypoint executable
RUN chmod +x /app/entrypoint.sh

EXPOSE 8000

# entrypoint.sh: waits for DB, runs Alembic migrations, then starts uvicorn.
# Any arguments are forwarded to uvicorn (e.g. --reload for development).
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--host", "0.0.0.0", "--port", "8000"]

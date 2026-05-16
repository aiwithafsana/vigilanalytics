#!/bin/sh
# entrypoint.sh — Run Alembic migrations then start uvicorn.
# All arguments are forwarded to uvicorn so docker-compose can pass --reload for dev.
set -e

echo "[entrypoint] Waiting for database…"
# pg_isready is not always available inside the container, so we retry via Python.
python - <<'EOF'
import os, time, asyncpg, asyncio

async def wait():
    url = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "")
    # url format: user:pass@host:port/db
    for attempt in range(30):
        try:
            conn = await asyncpg.connect(f"postgresql://{url}")
            await conn.close()
            print(f"[entrypoint] Database ready after {attempt + 1} attempt(s).")
            return
        except Exception as e:
            print(f"[entrypoint] DB not ready ({e}), retrying in 2s…")
            await asyncio.sleep(2)
    raise SystemExit("[entrypoint] Database never became ready — aborting.")

asyncio.run(wait())
EOF

echo "[entrypoint] Running Alembic migrations…"
alembic upgrade head

echo "[entrypoint] Starting Vigil API…"
exec uvicorn app.main:app "$@"

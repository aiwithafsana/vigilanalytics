import logging
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.config import get_settings
from app.database import engine, Base, get_db
from app.middleware.trusted_ip import get_real_ip
from app.logging_config import configure_logging
from app.routers import providers, cases, dashboard, users, audit, network
from app.routers import alerts
from app.routers import ws as ws_router
from app.routers import system as system_router
from app.routers import agents as agents_router
from app.routers import clusters as clusters_router
from app.routers import lead_packs as lead_packs_router
from app.middleware.audit import AuditMiddleware
from app.middleware.security import SecurityHeadersMiddleware

settings = get_settings()

# Structured JSON logging — must be configured before any logger.getLogger() calls.
configure_logging(level="DEBUG" if settings.app_env == "development" else "INFO")
logger = logging.getLogger(__name__)

# Rate limiter — uses direct TCP connection IP, NOT X-Forwarded-For.
# Trusting X-Forwarded-For is trivially bypassed: an attacker rotates the header
# value to circumvent per-IP limits.  In production behind a trusted reverse proxy,
# configure the proxy to overwrite (not append) the real client IP into a single
# dedicated header and update get_real_ip() accordingly.
limiter = Limiter(key_func=get_real_ip, default_limits=["300/minute"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio
    from app.cache import cache
    from app.database import AsyncSessionLocal

    # Dev convenience: create any missing tables on startup.
    # PRODUCTION SCHEMA CHANGES MUST FLOW THROUGH ALEMBIC — see db/migrations.
    # `create_all(checkfirst=True)` only creates missing TABLES; it cannot add
    # columns to existing tables and will silently leave schema drift in place.
    # Run `alembic upgrade head` before starting the app in production.
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, checkfirst=True))

    # Background task: purge stale cache entries every 10 minutes
    async def _cache_sweep():
        while True:
            await asyncio.sleep(600)
            removed = cache.purge_expired()
            if removed:
                logger.info("Cache sweep complete", extra={"removed": removed})

    # Background task: weekly LEIE refresh.
    # First run is delayed 5 min so the API is fully serving traffic before a
    # heavy download starts.  Then runs every 7 days.  The task catches and
    # logs all exceptions so a single failure never crashes the server.
    async def _leie_refresh_loop():
        from app.services.leie_refresh import refresh_leie
        WEEK_SECONDS = 7 * 24 * 60 * 60
        STARTUP_DELAY = 5 * 60      # 5 min after boot
        FAILURE_RETRY = 60 * 60     # 1 hour after a failure
        await asyncio.sleep(STARTUP_DELAY)
        while True:
            try:
                async with AsyncSessionLocal() as db:
                    delta = await refresh_leie(db)
                    await db.commit()
                logger.info(
                    "Scheduled LEIE refresh applied",
                    extra={
                        "newly_excluded":   delta.newly_excluded,
                        "newly_reinstated": delta.newly_reinstated,
                        "flags_inserted":   delta.flags_inserted,
                    },
                )
                await asyncio.sleep(WEEK_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Scheduled LEIE refresh failed; will retry in 1h")
                await asyncio.sleep(FAILURE_RETRY)

    # Background task: nightly case-watch sweep.
    # Runs the public-records agent against every open case's provider, so
    # the next morning each investigator's dashboard surfaces "what's new
    # since yesterday" deltas without manual polling.
    # First run delayed 15 min so morning traffic isn't competing with the
    # heavy outbound API calls.  Re-runs every 24h.  Failures retry in 2h.
    async def _case_watch_loop():
        from app.services.case_watch import run_nightly_watch
        DAY_SECONDS    = 24 * 60 * 60
        STARTUP_DELAY  = 15 * 60       # 15 min after boot
        FAILURE_RETRY  = 2 * 60 * 60   # 2 hours after a failure
        await asyncio.sleep(STARTUP_DELAY)
        while True:
            try:
                digest = await run_nightly_watch()
                logger.info(
                    "Scheduled case-watch sweep applied",
                    extra={
                        "n_cases_watched":      digest["n_cases_watched"],
                        "n_cases_with_changes": digest["n_cases_with_changes"],
                        "n_new_findings_total": digest["n_new_findings_total"],
                    },
                )
                await asyncio.sleep(DAY_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Scheduled case-watch sweep failed; will retry in 2h")
                await asyncio.sleep(FAILURE_RETRY)

    cache_task = asyncio.create_task(_cache_sweep())
    leie_task  = asyncio.create_task(_leie_refresh_loop())
    watch_task = asyncio.create_task(_case_watch_loop())
    yield
    cache_task.cancel()
    leie_task.cancel()
    watch_task.cancel()
    await engine.dispose()


app = FastAPI(
    title="Vigil API",
    description="Medicare Fraud Intelligence Platform",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

# ── Rate limiter ──────────────────────────────────────────────────────────────
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── CORS — restrictive ────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Accept"],
    expose_headers=["Content-Disposition"],  # needed for PDF/CSV downloads
    max_age=600,
)

# ── Security headers ──────────────────────────────────────────────────────────
app.add_middleware(SecurityHeadersMiddleware)

# ── Audit logging ─────────────────────────────────────────────────────────────
app.add_middleware(AuditMiddleware)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(providers.router, prefix="/api/providers", tags=["providers"])
app.include_router(cases.router, prefix="/api/cases", tags=["cases"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["dashboard"])
app.include_router(users.router, prefix="/api/users", tags=["users"])
app.include_router(audit.router, prefix="/api/audit", tags=["audit"])
app.include_router(network.router, prefix="/api/network", tags=["network"])
app.include_router(alerts.router, prefix="/api/alerts", tags=["alerts"])
app.include_router(ws_router.router, prefix="/api", tags=["realtime"])
app.include_router(system_router.router, prefix="/api/system", tags=["system"])
app.include_router(clusters_router.router, prefix="/api/clusters", tags=["clusters"])
app.include_router(lead_packs_router.router, prefix="/api/lead-packs", tags=["lead-packs"])
app.include_router(agents_router.router, prefix="/api/agents", tags=["agents"])


@app.get("/api/health", include_in_schema=False)
async def health():
    # Liveness probe — deliberately minimal, no external calls.
    return {"status": "ok"}


@app.get("/api/ready", include_in_schema=False)
async def ready(db: Annotated[AsyncSession, Depends(get_db)]):
    """
    Readiness probe — confirms the application can serve traffic.

    Returns 200 when the DB is reachable, 503 when it isn't.
    Kubernetes / Docker Swarm should gate traffic on this endpoint, not /api/health.
    """
    try:
        await db.execute(text("SELECT 1"))
        return {"status": "ready", "db": "ok"}
    except Exception as exc:  # noqa: BLE001
        logger.error("Readiness check failed", extra={"error": str(exc)})
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "db": "unreachable"},
        )

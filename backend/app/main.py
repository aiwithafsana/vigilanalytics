from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from app.config import get_settings
from app.database import engine, Base
from app.routers import providers, cases, dashboard, users, audit, network
from app.routers import alerts
from app.routers import ws as ws_router
from app.middleware.audit import AuditMiddleware
from app.middleware.security import SecurityHeadersMiddleware

settings = get_settings()

# Rate limiter (in-memory; use Redis backend in production)
limiter = Limiter(key_func=get_remote_address, default_limits=["300/minute"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio
    from app.cache import cache

    # Create tables on startup
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, checkfirst=True))

    # Background task: purge stale cache entries every 10 minutes
    async def _cache_sweep():
        while True:
            await asyncio.sleep(600)
            removed = cache.purge_expired()
            if removed:
                print(f"[cache] purged {removed} expired entries")

    task = asyncio.create_task(_cache_sweep())
    yield
    task.cancel()
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


@app.get("/api/health", include_in_schema=False)
async def health():
    # Deliberately minimal — do not expose env or internal state
    return {"status": "ok"}

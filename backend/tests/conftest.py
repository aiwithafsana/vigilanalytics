"""
conftest.py — Shared fixtures for the Vigil backend test suite.

IMPORTANT: os.environ assignments at module level must come first so that
every subsequent import of app.* picks up the test values.
"""
import os

# ── Point every app module at the test database ──────────────────────────────
# These must be set *before* any `from app.*` import executes.
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://vigil:vigil@localhost:5432/vigil_test")
os.environ.setdefault("SECRET_KEY", "test-secret-key-that-is-32-chars-longxxxxxx")  # ≥32 chars
os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("CORS_ORIGINS", '["http://localhost:3000"]')

import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

# Clear the lru_cache so get_settings() re-reads our test env vars.
from app.config import get_settings
get_settings.cache_clear()

from app.auth import create_access_token
from app.database import Base, get_db
from app.main import app
from app.models import User
from tests.factories import create_provider, create_user

# ── Test engine (independent from the app's pool) ─────────────────────────────
_TEST_DB_URL = os.environ["DATABASE_URL"]
# NullPool: every session gets a fresh connection — avoids asyncpg connections
# being recycled across different event loops (pytest-asyncio function vs session scopes).
_test_engine = create_async_engine(_TEST_DB_URL, echo=False, poolclass=NullPool)
_TestSession = async_sessionmaker(_test_engine, class_=AsyncSession, expire_on_commit=False)

# ── Table names in dependency order (FK children before parents) ──────────────
_TRUNCATE_SQL = text(
    "TRUNCATE TABLE "
    "audit_log, case_documents, case_notes, cases, "
    "fraud_flags, billing_records, referral_edges, "
    "leie_exclusions, dashboard_stats, peer_benchmarks, "
    "providers, users "
    "RESTART IDENTITY CASCADE"
)


# ── Session-scoped: create schema once, drop on exit ─────────────────────────

@pytest.fixture(scope="session", autouse=True)
async def _create_schema():
    async with _test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with _test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await _test_engine.dispose()


# ── Function-scoped: blank slate before every test ────────────────────────────

@pytest.fixture(autouse=True)
async def _clean_tables():
    """Truncate all tables before each test for full isolation."""
    async with _TestSession() as session:
        await session.execute(_TRUNCATE_SQL)
        await session.commit()
    yield


@pytest.fixture(autouse=True)
def _reset_rate_limiters():
    """
    Clear SlowAPI rate-limit counters before every test.

    All tests share the same in-process ASGI app (and therefore the same
    in-memory rate-limit storage).  Without a reset, earlier tests' login
    attempts accumulate against the 10/minute budget and cause lockout tests
    to receive 429 instead of the 423 they assert.

    Calling limiter.reset() is the official SlowAPI API for clearing storage.
    """
    from app.main import limiter as _app_limiter
    from app.routers.users import limiter as _users_limiter
    _app_limiter.reset()
    _users_limiter.reset()
    yield


# ── Core fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture
async def db() -> AsyncSession:
    """
    Direct DB access for test setup and assertions.
    Call `await db.commit()` after inserting test data so the HTTP client
    (which uses its own connection) can see the rows.
    """
    async with _TestSession() as session:
        yield session


@pytest.fixture
async def client() -> AsyncClient:
    """
    HTTPX async client backed by the test database.

    The get_db dependency is overridden so every request gets a proper
    session against the test DB (with commit-on-success / rollback-on-error).
    """

    async def _override_get_db():
        async with _TestSession() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = _override_get_db
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.pop(get_db, None)


# ── User fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture
async def admin_user(db) -> User:
    user = await create_user(db, email="admin@test.example", role="admin")
    await db.commit()
    return user


@pytest.fixture
async def analyst_user(db) -> User:
    user = await create_user(db, email="analyst@test.example", role="analyst")
    await db.commit()
    return user


@pytest.fixture
async def viewer_user(db) -> User:
    user = await create_user(db, email="viewer@test.example", role="viewer")
    await db.commit()
    return user


# ── Auth header helpers ────────────────────────────────────────────────────────

@pytest.fixture
def admin_headers(admin_user) -> dict:
    token = create_access_token(admin_user)
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def analyst_headers(analyst_user) -> dict:
    token = create_access_token(analyst_user)
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def viewer_headers(viewer_user) -> dict:
    token = create_access_token(viewer_user)
    return {"Authorization": f"Bearer {token}"}


# ── Shared data fixtures ──────────────────────────────────────────────────────

@pytest.fixture
async def test_provider(db):
    """A standard provider committed to the test DB."""
    provider = await create_provider(db, npi="1234567890", state="CA", risk_score=75.0)
    await db.commit()
    return provider

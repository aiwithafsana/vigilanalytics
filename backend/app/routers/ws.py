"""
ws.py — WebSocket endpoint for real-time fraud alert notifications.

GET /api/ws?token=<jwt>

Clients connect with their JWT access token as a query parameter (browsers can't
set Authorization headers on WebSocket handshakes). The server:
  1. Authenticates the token and looks up the user.
  2. Sends a `connected` confirmation.
  3. Polls the database every 30 seconds for new high-severity fraud flags
     created since the connection opened, filtered to the user's jurisdiction.
  4. Sends `new_alerts` messages when flags are found, otherwise a `ping`.
  5. Cleans up on disconnect.

Message shapes:
  → connected   { type, user_email, active_connections }
  → new_alerts  { type, count, alerts: AlertItem[] }
  → ping        { type, ts }
  ← pong        (client may send to keep-alive — server ignores body)
"""
from __future__ import annotations

import asyncio
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.auth import _create_token, decode_token, get_current_user
from app.cache import cache
from app.config import get_settings
from app.database import AsyncSessionLocal
from app.models import FraudFlag, Provider, User
from app.ws_manager import ws_manager

logger = logging.getLogger(__name__)

router = APIRouter()
settings = get_settings()

# Poll interval in seconds — balance freshness vs DB load
_POLL_INTERVAL = 30

# Only push critical (1) and high (2) flags over the wire
_MAX_SEVERITY = 2


async def _fetch_new_alerts(
    session_factory: async_sessionmaker,
    user: User,
    since: datetime,
    limit: int = 15,
) -> list[dict]:
    """Query DB for new active fraud flags since `since` for this user."""
    async with session_factory() as db:
        q = (
            select(FraudFlag, Provider)
            .join(Provider, FraudFlag.npi == Provider.npi)
            .where(FraudFlag.is_active == True)          # noqa: E712
            .where(FraudFlag.severity <= _MAX_SEVERITY)
            .where(FraudFlag.created_at > since)
        )
        allowed = user.state_access or []
        if allowed:
            q = q.where(Provider.state.in_(allowed))

        q = q.order_by(FraudFlag.severity.asc(), FraudFlag.created_at.desc()).limit(limit)
        rows = (await db.execute(q)).all()

        result = []
        for flag, provider in rows:
            name = (
                f"{provider.name_first or ''} {provider.name_last or ''}".strip()
                or provider.name_last
                or provider.npi
            )
            result.append({
                "flag_id": flag.id,
                "npi": flag.npi,
                "provider_name": name,
                "specialty": provider.specialty,
                "state": provider.state,
                "risk_score": float(provider.risk_score) if provider.risk_score else 0.0,
                "flag_type": flag.flag_type,
                "severity": flag.severity,
                "explanation": flag.explanation,
                "estimated_overpayment": (
                    float(flag.estimated_overpayment) if flag.estimated_overpayment else None
                ),
                "created_at": flag.created_at.isoformat() if flag.created_at else None,
            })
        return result


_WS_TICKET_TTL = 30      # seconds a WS ticket remains valid
_WS_TICKET_CACHE_KEY = "ws_ticket_used:"


@router.get("/ws/ticket")
async def get_ws_ticket(
    current_user: Annotated[User, Depends(get_current_user)],
) -> dict:
    """
    Issue a short-lived (30-second), single-use WebSocket authentication ticket.

    Browsers cannot set Authorization headers on WebSocket connections, so a naive
    implementation passes the main JWT in the query string — where it appears in
    server access logs, browser history, and Referrer headers.

    The safe pattern:
      1. Client calls GET /api/ws/ticket with the access token in the Authorization header.
      2. Server returns a one-time ticket that expires in 30 seconds.
      3. Client opens: ws://host/api/ws?ticket=<ticket>
      4. Server validates the ticket, marks it used (cannot be replayed), and upgrades.

    The ticket carries a random jti (JWT ID) nonce.  On use the server records the
    nonce in the cache; any subsequent connection attempt with the same ticket is
    rejected even within the 30-second window.
    """
    jti = secrets.token_hex(16)
    ticket = _create_token(
        {
            "sub": str(current_user.id),
            "type": "ws_ticket",
            "jti": jti,
            "ver": current_user.token_version,
        },
        timedelta(seconds=_WS_TICKET_TTL),
    )
    return {"ticket": ticket, "expires_in": _WS_TICKET_TTL}


@router.websocket("/ws")
async def websocket_alerts(
    websocket: WebSocket,
    token: str | None = Query(None, description="JWT access token (deprecated — use ticket=)"),
    ticket: str | None = Query(None, description="Short-lived WS ticket from GET /api/ws/ticket"),
):
    # ── Authenticate ──────────────────────────────────────────────────────────
    try:
        raw = ticket or token
        if not raw:
            await websocket.close(code=4001, reason="Authentication required")
            return

        payload = decode_token(raw)
        token_type = payload.get("type")

        if token_type == "ws_ticket":
            # Single-use ticket: mark jti as consumed before any I/O to prevent
            # a race condition where two concurrent connections use the same ticket.
            jti = payload.get("jti")
            if not jti:
                await websocket.close(code=4001, reason="Malformed ticket")
                return
            cache_key = f"{_WS_TICKET_CACHE_KEY}{jti}"
            if await cache.get(cache_key) is not None:
                await websocket.close(code=4001, reason="Ticket already used")
                return
            # Mark as used for the duration of the ticket TTL
            await cache.set(cache_key, True, ttl=_WS_TICKET_TTL)

        elif token_type == "access":
            pass  # accepted but discouraged (exposes token in URL/logs)
        else:
            await websocket.close(code=4001, reason="Invalid token type")
            return

        user_id_str: str = payload.get("sub", "")
        user_uuid = UUID(user_id_str)
    except Exception as exc:
        logger.warning("WebSocket auth failed", extra={"error": str(exc)})
        await websocket.close(code=4001, reason="Authentication failed")
        return

    # Re-use the module-level session factory — creating a new async_sessionmaker
    # per connection would spawn an independent connection pool per client and
    # exhaust PostgreSQL's max_connections under any real load.
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(User).where(User.id == user_uuid))
        user = result.scalar_one_or_none()

    if not user or not user.is_active:
        await websocket.close(code=4001, reason="User not found or inactive")
        return

    # Validate token version (same revocation check as get_current_user)
    token_ver = payload.get("ver")
    if token_ver is None or token_ver != user.token_version:
        await websocket.close(code=4001, reason="Session revoked")
        return

    # ── Connect ───────────────────────────────────────────────────────────────
    conn_id = await ws_manager.connect(websocket, str(user_uuid))

    try:
        await websocket.send_json({
            "type": "connected",
            "user_email": user.email,
            "active_connections": ws_manager.active_count,
        })

        # Baseline: show alerts from the last 5 minutes on first poll
        since = datetime.now(timezone.utc) - timedelta(minutes=5)

        while True:
            # Wait — but also listen for incoming frames (pong / close)
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=_POLL_INTERVAL)
                # If we get here the client sent a message — reset the timer
            except asyncio.TimeoutError:
                pass  # Normal — poll time elapsed

            now = datetime.now(timezone.utc)
            alerts = await _fetch_new_alerts(AsyncSessionLocal, user, since)
            since = now

            if alerts:
                await websocket.send_json({
                    "type": "new_alerts",
                    "count": len(alerts),
                    "alerts": alerts,
                })
            else:
                await websocket.send_json({
                    "type": "ping",
                    "ts": now.isoformat(),
                })

    except WebSocketDisconnect:
        pass  # normal client disconnect
    except Exception as exc:
        logger.error(
            "WebSocket session error",
            extra={"user_id": str(user_uuid), "error": str(exc)},
            exc_info=True,
        )
    finally:
        ws_manager.disconnect(conn_id, str(user_uuid))

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
from datetime import datetime, timedelta, timezone
from uuid import UUID

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.auth import decode_token
from app.config import get_settings
from app.database import engine
from app.models import FraudFlag, Provider, User
from app.ws_manager import ws_manager

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


@router.websocket("/ws")
async def websocket_alerts(
    websocket: WebSocket,
    token: str = Query(..., description="JWT access token"),
):
    # ── Authenticate ──────────────────────────────────────────────────────────
    try:
        payload = decode_token(token)
        if payload.get("type") != "access":
            await websocket.close(code=4001, reason="Invalid token type")
            return
        user_id_str: str = payload.get("sub", "")
        user_uuid = UUID(user_id_str)
    except Exception:
        await websocket.close(code=4001, reason="Authentication failed")
        return

    Session: async_sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as db:
        result = await db.execute(select(User).where(User.id == user_uuid))
        user = result.scalar_one_or_none()

    if not user or not user.is_active:
        await websocket.close(code=4001, reason="User not found or inactive")
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
            alerts = await _fetch_new_alerts(Session, user, since)
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
        pass
    except Exception:
        pass
    finally:
        ws_manager.disconnect(conn_id, str(user_uuid))

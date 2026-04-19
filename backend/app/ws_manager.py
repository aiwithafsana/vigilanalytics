"""
ws_manager.py — WebSocket connection registry.

Tracks all active WebSocket connections keyed by a generated connection ID.
Allows broadcasting to all connections or targeting a specific user's connections.
"""
from __future__ import annotations

import uuid
from collections import defaultdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import WebSocket


class ConnectionManager:
    def __init__(self) -> None:
        # conn_id → WebSocket
        self._connections: dict[str, "WebSocket"] = {}
        # user_id → set of conn_ids  (one user can have multiple browser tabs)
        self._by_user: dict[str, set[str]] = defaultdict(set)

    async def connect(self, websocket: "WebSocket", user_id: str) -> str:
        """Accept the connection and register it. Returns the conn_id."""
        await websocket.accept()
        conn_id = str(uuid.uuid4())
        self._connections[conn_id] = websocket
        self._by_user[user_id].add(conn_id)
        return conn_id

    def disconnect(self, conn_id: str, user_id: str) -> None:
        self._connections.pop(conn_id, None)
        self._by_user[user_id].discard(conn_id)
        if not self._by_user[user_id]:
            del self._by_user[user_id]

    async def send(self, conn_id: str, message: dict) -> bool:
        """Send JSON to one connection. Returns False if the socket is gone."""
        ws = self._connections.get(conn_id)
        if ws is None:
            return False
        try:
            await ws.send_json(message)
            return True
        except Exception:
            return False

    async def broadcast(self, message: dict) -> int:
        """Send to every connected client. Returns number of successful sends."""
        dead: list[tuple[str, str]] = []
        sent = 0
        for conn_id, ws in list(self._connections.items()):
            try:
                await ws.send_json(message)
                sent += 1
            except Exception:
                # Find which user owns this dead connection
                for uid, conns in self._by_user.items():
                    if conn_id in conns:
                        dead.append((conn_id, uid))
                        break
        for conn_id, uid in dead:
            self.disconnect(conn_id, uid)
        return sent

    @property
    def active_count(self) -> int:
        return len(self._connections)

    @property
    def user_count(self) -> int:
        return len(self._by_user)


# ── Singleton ─────────────────────────────────────────────────────────────────
ws_manager = ConnectionManager()

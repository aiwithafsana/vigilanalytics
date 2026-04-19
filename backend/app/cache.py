"""
cache.py — In-process TTL cache.

Single-process, asyncio-safe. Resets on server restart.
Good for read-heavy endpoints that aggregate 1M+ rows (fraud map, top providers).
Not suitable for user-specific write-invalidated data.

Usage:
    from app.cache import cache

    result = await cache.get("map:all")
    if result is None:
        result = expensive_query()
        await cache.set("map:all", result, ttl=300)
    return result
"""
import asyncio
import time
from typing import Any

class TTLCache:
    def __init__(self) -> None:
        self._store: dict[str, tuple[Any, float]] = {}  # key → (value, expire_monotonic)
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        async with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expire_at = entry
            if time.monotonic() > expire_at:
                del self._store[key]
                return None
            return value

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """Store value with TTL in seconds (default 5 min)."""
        async with self._lock:
            self._store[key] = (value, time.monotonic() + ttl)

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._store.pop(key, None)

    async def invalidate_prefix(self, prefix: str) -> int:
        """Delete all keys starting with prefix. Returns count deleted."""
        async with self._lock:
            victims = [k for k in self._store if k.startswith(prefix)]
            for k in victims:
                del self._store[k]
            return len(victims)

    def purge_expired(self) -> int:
        """Synchronously remove expired entries. Call from a cleanup task."""
        now = time.monotonic()
        victims = [k for k, (_, exp) in self._store.items() if now > exp]
        for k in victims:
            del self._store[k]
        return len(victims)

    @property
    def size(self) -> int:
        return len(self._store)


# ── Singleton ─────────────────────────────────────────────────────────────────
cache = TTLCache()

"""
logging_config.py — Structured JSON logging for Vigil.

Call configure_logging() once at application startup (main.py lifespan).
All subsequent uses of logging.getLogger(__name__) will emit single-line
JSON objects, making logs machine-parseable in Docker / Kubernetes while
remaining readable when piped through `jq`.

Usage in any module:
    import logging
    logger = logging.getLogger(__name__)
    logger.info("cache purged", extra={"removed": 5})
"""
from __future__ import annotations

import json
import logging
import traceback
from typing import Any

# Standard LogRecord attributes — we exclude these to avoid double-encoding.
_STDLIB_ATTRS = frozenset(
    (
        "name", "msg", "args", "levelname", "levelno", "pathname",
        "filename", "module", "exc_info", "exc_text", "stack_info",
        "lineno", "funcName", "created", "msecs", "relativeCreated",
        "thread", "threadName", "processName", "process",
        "taskName", "message",
    )
)


class JSONFormatter(logging.Formatter):
    """Emit each log record as a single compact JSON line."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        obj: dict[str, Any] = {
            "ts":     self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level":  record.levelname,
            "logger": record.name,
            "msg":    record.getMessage(),
        }

        if record.exc_info:
            obj["exc"] = "".join(traceback.format_exception(*record.exc_info)).strip()

        # Merge any extra={...} fields the caller supplied
        for key, val in record.__dict__.items():
            if key not in _STDLIB_ATTRS:
                obj[key] = val

        return json.dumps(obj, default=str)


def configure_logging(level: str = "INFO") -> None:
    """
    Install the JSON formatter on the root logger.

    Call once during application startup.  Idempotent — safe to call
    multiple times (replaces existing handlers each time).
    """
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Silence overly verbose third-party loggers
    for noisy in (
        "uvicorn.access",       # access log handled separately
        "sqlalchemy.engine",    # SQL echoing — too chatty in INFO mode
        "asyncio",
        "multipart",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)

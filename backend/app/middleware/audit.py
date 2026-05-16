"""
Starlette middleware that logs write operations (POST/PUT/PATCH/DELETE) to stdout.
Read-only routes are not logged here; explicit audit entries are added inside routers.
"""
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
SKIP_PATHS = {"/api/health", "/api/ready", "/api/docs", "/api/redoc", "/openapi.json"}


class AuditMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        # BaseHTTPMiddleware cannot handle WebSocket upgrade requests — pass them through
        if request.headers.get("upgrade", "").lower() == "websocket":
            return await call_next(request)

        response = await call_next(request)

        if request.method in WRITE_METHODS and request.url.path not in SKIP_PATHS:
            # Best-effort: structured log so aggregators (ELK, Loki, CloudWatch) can
            # parse and alert on write operations.
            # Full DB-backed audit entries are written inside individual routers.
            logger.info(
                "write_request",
                extra={
                    "method":     request.method,
                    "path":       request.url.path,
                    "status":     response.status_code,
                    "client_ip":  request.client.host if request.client else "unknown",
                },
            )

        return response

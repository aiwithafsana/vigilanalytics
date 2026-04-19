"""
Starlette middleware that logs write operations (POST/PUT/PATCH/DELETE) to audit_log.
Read-only routes are not logged here; explicit audit entries are added inside routers.
"""
import json
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
SKIP_PATHS = {"/api/health", "/api/docs", "/api/redoc", "/openapi.json"}


class AuditMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)

        if request.method in WRITE_METHODS and request.url.path not in SKIP_PATHS:
            # Best-effort: log to stdout so it can be captured by log aggregators.
            # Full DB-backed audit entries are written inside individual routers where
            # we have the DB session and user context.
            path = request.url.path
            method = request.method
            status_code = response.status_code
            client_ip = request.client.host if request.client else "unknown"
            print(f"[AUDIT] {method} {path} → {status_code} from {client_ip}")

        return response

"""
security.py — Security headers middleware.

Adds OWASP-recommended response headers to every request:
  - Strict-Transport-Security  (HSTS)
  - X-Content-Type-Options     (no MIME sniffing)
  - X-Frame-Options            (clickjacking protection)
  - X-XSS-Protection           (legacy IE protection)
  - Referrer-Policy
  - Content-Security-Policy    (restrictive for API; relaxed for Swagger docs)
  - Permissions-Policy
  - Cache-Control              (no caching for API responses)
"""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# Endpoints that serve Swagger UI (need inline scripts / CDN resources)
_DOC_PATHS = {"/api/docs", "/api/redoc", "/openapi.json"}

_CSP_API = (
    "default-src 'none'; "
    "frame-ancestors 'none';"
)

_CSP_DOCS = (
    "default-src 'self' https://cdn.jsdelivr.net https://fastapi.tiangolo.com; "
    "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
    "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://fonts.googleapis.com; "
    "img-src 'self' data: https://fastapi.tiangolo.com; "
    "font-src https://fonts.gstatic.com; "
    "frame-ancestors 'none';"
)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        response: Response = await call_next(request)

        is_docs = request.url.path in _DOC_PATHS

        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = (
            "geolocation=(), camera=(), microphone=(), payment=()"
        )
        response.headers["Content-Security-Policy"] = _CSP_DOCS if is_docs else _CSP_API

        # Only send HSTS in production (avoids localhost HTTPS issues)
        if request.headers.get("x-forwarded-proto") == "https":
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains; preload"
            )

        # Prevent caching of API responses (sensitive data)
        if request.url.path.startswith("/api/") and not is_docs:
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, private"
            response.headers["Pragma"] = "no-cache"

        return response

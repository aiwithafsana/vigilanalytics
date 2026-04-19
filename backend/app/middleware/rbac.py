"""
RBAC permission matrix:

Role       | providers | cases         | users        | audit
-----------+-----------+---------------+--------------+--------
admin      | R/W       | R/W + assign  | R/W          | R
analyst    | R         | R/W (own)     | R (self)     | -
viewer     | R         | R             | R (self)     | -
"""

ROLE_PERMISSIONS: dict[str, set[str]] = {
    "admin": {
        "providers:read",
        "providers:export",
        "cases:read",
        "cases:write",
        "cases:assign",
        "cases:export",
        "users:read",
        "users:write",
        "audit:read",
        "data:ingest",
    },
    "analyst": {
        "providers:read",
        "providers:export",
        "cases:read",
        "cases:write",
        "cases:export",
    },
    "viewer": {
        "providers:read",
        "cases:read",
    },
}


def has_permission(role: str, permission: str) -> bool:
    return permission in ROLE_PERMISSIONS.get(role, set())

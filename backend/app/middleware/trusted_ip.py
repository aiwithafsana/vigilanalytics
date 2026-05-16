"""
trusted_ip.py — Rate-limit key function that resists X-Forwarded-For spoofing.

get_remote_address (slowapi's default) reads X-Forwarded-For, which any HTTP
client can set to any value.  An attacker can bypass per-IP rate limits by
cycling through fake IPs in that header.

get_real_ip() uses request.client.host — the actual TCP peer address, which
is set by the OS/ASGI server from the socket and cannot be spoofed by the client.

In production behind a load balancer / CDN:
  1. Configure the LB to inject the real client IP into a dedicated header
     (e.g. CF-Connecting-IP, True-Client-IP, or X-Real-IP) and to STRIP any
     client-supplied header with the same name.
  2. Replace the body of this function to read that header instead.
  3. Never fall back to X-Forwarded-For — it accumulates proxy addresses and
     the leftmost value is client-controlled when not behind a trusted proxy.
"""
from fastapi import Request


def get_real_ip(request: Request) -> str:
    """Return the direct TCP peer address, ignoring forwarding headers."""
    if request.client:
        return request.client.host
    return "unknown"

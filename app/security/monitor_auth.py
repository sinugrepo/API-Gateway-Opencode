"""Monitor dashboard cookie auth (HMAC, no server session)."""
import hashlib
import hmac
import time
from typing import Dict, Optional

from fastapi import Request

from app.core.config import MONITOR_COOKIE_NAME, MONITOR_PASSWORD, MONITOR_SECRET, MONITOR_TOKEN_TTL


def _make_monitor_token() -> str:
    expiry = int(time.time()) + MONITOR_TOKEN_TTL
    raw = f"{expiry}:{MONITOR_PASSWORD}"
    sig = hmac.new(
        MONITOR_SECRET.encode(),
        raw.encode(),
        hashlib.sha256,
    ).hexdigest()
    return f"{expiry}:{sig}"


def _verify_monitor_token(token: str) -> bool:
    try:
        parts = token.split(":")
        expiry = int(parts[0])
        sig = parts[1]
    except (IndexError, ValueError):
        return False
    if time.time() > expiry:
        return False
    expected = hmac.new(
        MONITOR_SECRET.encode(),
        f"{expiry}:{MONITOR_PASSWORD}".encode(),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(sig, expected)


def _check_monitor(request: Request) -> bool:
    token = request.cookies.get(MONITOR_COOKIE_NAME)
    if not token:
        return False
    return _verify_monitor_token(token)

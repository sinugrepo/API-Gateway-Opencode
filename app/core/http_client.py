"""Shared httpx client (pooling) + cached async DNS."""
import asyncio
import socket
import threading
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

import httpx

from app.core.config import REQUEST_TIMEOUT


_shared_http: Optional[httpx.AsyncClient] = None


_shared_http_lock = threading.Lock()


_shared_http_closed = threading.Event()


_HTTP_LIMITS = httpx.Limits(
    max_keepalive_connections=20,
    max_connections=100,
    keepalive_expiry=30.0,
)


def _get_http() -> httpx.AsyncClient:
    """Return a shared persistent httpx.AsyncClient with connection pooling."""
    global _shared_http
    with _shared_http_lock:
        if _shared_http is None or _shared_http_closed.is_set():
            _shared_http = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=float(REQUEST_TIMEOUT),
                    write=10.0,
                    pool=10.0,
                ),
                limits=_HTTP_LIMITS,
            )
            _shared_http_closed.clear()
        return _shared_http


async def _close_http() -> None:
    global _shared_http
    with _shared_http_lock:
        if _shared_http is not None:
            await _shared_http.aclose()
            _shared_http = None
            _shared_http_closed.set()


_dns_cache: Dict[str, Tuple[float, str]] = {}


_dns_cache_lock = threading.Lock()


_DNS_CACHE_TTL = 300


async def _resolve_relay_ip(relay_url: str) -> Optional[str]:
    """Resolve relay hostname to IP via async DNS, cached for _DNS_CACHE_TTL seconds."""
    host = urlparse(relay_url).hostname
    if not host:
        return None
    now = time.time()
    with _dns_cache_lock:
        if host in _dns_cache:
            cached_at, ip = _dns_cache[host]
            if now - cached_at < _DNS_CACHE_TTL:
                return ip if ip else None
    try:
        ip = await asyncio.to_thread(socket.gethostbyname, host)
    except OSError:
        ip = None
    with _dns_cache_lock:
        _dns_cache[host] = (time.time(), ip or "")
    return ip

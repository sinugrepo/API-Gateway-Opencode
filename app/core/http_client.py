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
    max_keepalive_connections=50,
    max_connections=200,
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
    # Tutup juga semua pooled proxy clients (hindari leak fd saat shutdown).
    await _close_all_proxy_clients()


# ---- Pooled proxy clients (satu AsyncClient per proxy URL) ----
# httpx mengikat `proxy=` pada level client, bukan request — jadi tiap proxy
# butuh client sendiri agar connection-pooling tetap jalan. Dict di-cache
# selamanya (masa hidup proses); entri dihapus saat proxy dihapus via UI
# (_close_proxy_client) atau saat shutdown (_close_all_proxy_clients).
_proxy_clients: Dict[str, httpx.AsyncClient] = {}

_proxy_clients_lock = threading.Lock()


def _get_proxy_client(proxy_url: str) -> httpx.AsyncClient:
    """Return pooled client yang request-nya keluar via `proxy_url`.

    Melempar ImportError bila skema socks dipakai tanpa `socksio` terpasang
    (pemanggil request-path WAJIB menangkap dan failover ke direct).
    Melempar ValueError bila proxy_url kosong/invalid.
    """
    cleaned = (proxy_url or "").strip()
    if not cleaned:
        raise ValueError("empty proxy url")
    with _proxy_clients_lock:
        existing = _proxy_clients.get(cleaned)
        if existing is not None:
            return existing
        # Konstruksi di dalam lock agar dua thread tak membuat ganda.
        # ImportError socksio muncul DI SINI (bukan saat request) — biarkan
        # naik agar pemanggil bisa menandai proxy gagal + pakai direct.
        client = httpx.AsyncClient(
            proxy=cleaned,
            timeout=httpx.Timeout(
                connect=10.0,
                read=float(REQUEST_TIMEOUT),
                write=10.0,
                pool=10.0,
            ),
            limits=_HTTP_LIMITS,
        )
        _proxy_clients[cleaned] = client
        return client


def _pick_http_client(proxy_url: Optional[str] = None) -> httpx.AsyncClient:
    """Pilih pooled client: via proxy bila diminta, else direct shared.

    Helper kecil agar 4 generator tidak mengulang try/except yang sama.
    TIDAK pernah melempar untuk proxy kosong/None (kembali direct).
    Untuk proxy non-kosong yang gagal konstruksi (mis. socksio hilang),
    ImportError/ValueError DIBIARKAN naik agar pemanggil failover eksplisit.
    """
    if proxy_url:
        return _get_proxy_client(proxy_url)
    return _get_http()


async def _close_proxy_client(proxy_url: str) -> None:
    """Tutup + buang pooled client satu proxy (dipakai saat proxy dihapus)."""
    cleaned = (proxy_url or "").strip()
    if not cleaned:
        return
    with _proxy_clients_lock:
        client = _proxy_clients.pop(cleaned, None)
    if client is not None:
        try:
            await client.aclose()
        except (RuntimeError, OSError, AttributeError):
            pass


async def _close_all_proxy_clients() -> None:
    with _proxy_clients_lock:
        clients = list(_proxy_clients.values())
        _proxy_clients.clear()
    for client in clients:
        try:
            await client.aclose()
        except (RuntimeError, OSError, AttributeError):
            pass


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

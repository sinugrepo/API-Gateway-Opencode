"""Fail2ban-style scan guard (ASGI middleware, streaming-safe)."""
import re
import threading
import time
from typing import Any, Dict, List

from app.core.config import (
    SCAN_GUARD_BAN_SECONDS,
    SCAN_GUARD_ENABLED,
    SCAN_GUARD_MAX_IPS,
    SCAN_GUARD_THRESHOLD,
    SCAN_GUARD_TRUST_PROXY,
    SCAN_GUARD_WINDOW,
)
from app.core.logging_utils import _log


# Pola path pemindai kredensial/backup. Disengaja tidak mencakup path API
# legit (/v1/*, /monitor, /health, ...) jadi klien normal takkan kena.
_SUSPICIOUS_PATH_RE = re.compile(
    r"(?i)(?:"
    r"\.env(?:\.|$|/)|\.aws/credentials|terraform\.tfstate"
    r"|config\.json|docker-compose\.ya?ml|application\.ya?ml|serverless\.ya?ml"
    r"|secrets?\.ya?ml|credentials\.json|\.npmrc|\.pypirc|\.gitconfig"
    r"|id_rsa|\.pem(?:$|/)|\.key(?:$|/)"
    r"|\.git(?:/|$)|\.svn(?:/|$)|\.hg(?:/|$)"
    r"|wp-admin|wp-login|phpmyadmin|phpinfo"
    r"|server-status|actuator|/console/|_profiler"
    r"|\.bak$|\.old$|\.save$|\.tmp$|\.swp$|\.orig$|\.copy$|\.backup$"
    r"|\.backup_|~$|\.1$"
    r"|\.DS_Store|Thumbs\.db"
    r")"
)


_scan_hits: Dict[str, List[float]] = {}


_banned_ips: Dict[str, float] = {}


_scan_guard_lock = threading.Lock()


def _scan_guard_prune(now: float) -> None:
    """Buang ban kedaluwarsa + timestamp di luar jendela; batasi ukuran.

    Perf: versi lama membangun list sementara per IP hanya untuk cek stale
    (O(total_hits) tiap probe). Versi ini filter in-place satu pass dan
    menghapus IP kosong — tanpa alokasi list ganda.
    """
    for ip in [ip for ip, until in _banned_ips.items() if until <= now]:
        del _banned_ips[ip]
    cutoff = now - SCAN_GUARD_WINDOW
    for ip in list(_scan_hits.keys()):
        hits = _scan_hits.get(ip)
        if not hits:
            _scan_hits.pop(ip, None)
            continue
        kept = [t for t in hits if t >= cutoff]
        if kept:
            if len(kept) != len(hits):
                _scan_hits[ip] = kept
        else:
            del _scan_hits[ip]
    while len(_scan_hits) > SCAN_GUARD_MAX_IPS:
        _scan_hits.pop(next(iter(_scan_hits)))
    while len(_banned_ips) > SCAN_GUARD_MAX_IPS:
        _banned_ips.pop(next(iter(_banned_ips)))


def _client_ip_from_scope(scope: Dict[str, Any]) -> str:
    """Ambil IP klien dari ASGI scope ( hormati XFF bila dikonfigurasi )."""
    if SCAN_GUARD_TRUST_PROXY:
        try:
            for raw_key, raw_val in scope.get("headers", []):
                if raw_key.decode("latin-1").lower() == "x-forwarded-for":
                    first = raw_val.decode("latin-1").split(",")[0].strip()
                    if first:
                        return first
        except (AttributeError, ValueError, IndexError, UnicodeDecodeError):
            pass
    try:
        client = scope.get("client")
        if client and client[0]:
            return str(client[0])
    except (TypeError, IndexError):
        pass
    return "unknown"


def _is_ip_banned(ip: str) -> bool:
    with _scan_guard_lock:
        until = _banned_ips.get(ip)
        if until is None:
            return False
        if time.time() >= until:
            del _banned_ips[ip]
            return False
        return True


def _record_probe(ip: str) -> bool:
    """Catat satu probe; kembalikan True bila IP baru saja melewati ambang."""
    now = time.time()
    with _scan_guard_lock:
        _scan_guard_prune(now)
        hits = _scan_hits.get(ip, [])
        hits = [t for t in hits if t >= now - SCAN_GUARD_WINDOW]
        hits.append(now)
        _scan_hits[ip] = hits
        if len(hits) >= SCAN_GUARD_THRESHOLD and ip not in _banned_ips:
            _banned_ips[ip] = now + SCAN_GUARD_BAN_SECONDS
            _log("WARN", f"SCAN-GUARD banned {ip} ({len(hits)} probes/{SCAN_GUARD_WINDOW:.0f}s)")
            return True
        return False


class ScanGuardMiddleware:
    """ASGI middleware murni: blokir pemindai kredensial + IP yang di-ban.

    Sengaja bukan BaseHTTPMiddleware agar streaming SSE tidak terganggu:
    request legit diteruskan apa adanya tanpa menyentuh body.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(
        self, scope: Dict[str, Any], receive: Any, send: Any
    ) -> None:
        if scope.get("type") != "http" or not SCAN_GUARD_ENABLED:
            await self.app(scope, receive, send)
            return
        ip = _client_ip_from_scope(scope)
        if _is_ip_banned(ip):
            await self._reject(send, 403, b'{"error":{"message":"Forbidden","code":"BANNED"}}')
            return
        path = scope.get("path") or ""
        if isinstance(path, str) and _SUSPICIOUS_PATH_RE.search(path):
            just_banned = _record_probe(ip)
            status = 403 if just_banned or _is_ip_banned(ip) else 404
            body = (
                b'{"error":{"message":"Forbidden","code":"BANNED"}}'
                if status == 403 else b'{"detail":"Not Found"}'
            )
            await self._reject(send, status, body)
            return
        await self.app(scope, receive, send)

    @staticmethod
    async def _reject(send: Any, status: int, body: bytes) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": body})

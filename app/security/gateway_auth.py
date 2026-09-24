"""Gateway API-key auth untuk traffic luar (OpenAI-compatible).

Klien mengirim SATU dari:
  Authorization: Bearer <key>   (standar OpenAI/SDK/Hermes)
  x-api-key: <key>              (alias umum gateway)
  ?api_key=<key>                (fallback curl/browser, tidak disarankan)

Aktif HANYA bila `.env` / env mengisi key:
  GATEWAY_API_KEYS="sk-aaa,sk-bbb"  (utama, multi-key/rotasi)
  GATEWAY_API_KEY="sk-aaa"          (satu key)
  API_KEYS="sk-aaa"                 (alias)
Kosong = mode TERBUKA (backward-compat lokal). Terisi = enforce 401.

401 memakai HTTPException agar error_handlers membentuk
`{"error": {...}}` OpenAI-compatible seperti error lain.
"""
import hmac
from typing import Optional

from fastapi import HTTPException, Request
from starlette.status import HTTP_401_UNAUTHORIZED

from app.core.logging_utils import _log


def _extract_gateway_key(request: Request) -> str:
    """Ambil kandidat key dari header/query, tanpa validasi."""
    try:
        headers = getattr(request, "headers", None) or {}
        # Authorization: Bearer <key> (case-insensitive scheme, toleran spasi)
        auth = ""
        try:
            auth = headers.get("authorization", "") or ""
        except (AttributeError, TypeError):
            auth = ""
        if isinstance(auth, str) and auth.strip():
            scheme, _, token = auth.strip().partition(" ")
            if scheme.lower() == "bearer" and token.strip():
                return token.strip()
        # x-api-key (FastAPI headers sudah case-insensitive; dict biasa belum)
        for name in ("x-api-key", "x_api_key"):
            try:
                value = headers.get(name, "")
            except (AttributeError, TypeError):
                value = ""
            if isinstance(value, str) and value.strip():
                return value.strip()
        # Fallback dict case-sensitive manual (untuk SimpleNamespace di test)
        try:
            items = headers.items()
        except (AttributeError, TypeError):
            items = ()
        for key, val in items:
            try:
                if str(key).lower() == "x-api-key" and isinstance(val, str) and val.strip():
                    return val.strip()
            except (TypeError, ValueError):
                continue
        # Query ?api_key= (terakhir, agar header diutamakan)
        try:
            params = getattr(request, "query_params", None) or {}
            for qname in ("api_key", "key"):
                qval = params.get(qname, "")
                if isinstance(qval, str) and qval.strip():
                    return qval.strip()
        except (AttributeError, TypeError):
            pass
    except (AttributeError, TypeError, ValueError):
        pass
    return ""


def _client_ip(request: Request) -> str:
    try:
        client = getattr(request, "client", None)
        if client and getattr(client, "host", None):
            return str(client.host)
    except (AttributeError, TypeError, ValueError):
        pass
    return "?"


def is_gateway_auth_enabled() -> bool:
    # Import di dalam fungsi agar test bisa monkeypatch env + reload config
    # tanpa circular import (gateway_auth <- routes <- app).
    try:
        from app.core.config import GATEWAY_AUTH_ENABLED
        return bool(GATEWAY_AUTH_ENABLED)
    except (ImportError, AttributeError):
        return False


def _is_valid_gateway_key(candidate: str) -> bool:
    if not candidate:
        return False
    try:
        from app.core.config import GATEWAY_API_KEYS
        allowed = GATEWAY_API_KEYS or []
    except (ImportError, AttributeError):
        return False
    for valid in allowed:
        try:
            if valid and hmac.compare_digest(candidate, valid):
                return True
        except (TypeError, ValueError):
            continue
    return False


async def verify_gateway_key(request: Request) -> str:
    """FastAPI dependency: loloskan bila key valid / auth nonaktif.

    Return key yang dipakai ("" bila mode terbuka). Raise 401 bila:
    - auth aktif tapi key hilang, atau
    - key tidak cocok dengan daftar mana pun.
    Key TIDAK pernah ditulis ke log (hanya prefix 4 char untuk debug).
    """
    if not is_gateway_auth_enabled():
        return ""
    candidate = _extract_gateway_key(request)
    if _is_valid_gateway_key(candidate):
        return candidate
    ip = _client_ip(request)
    try:
        path = getattr(request, "url", None)
        path_str = getattr(path, "path", "?") if path is not None else "?"
    except (AttributeError, TypeError, ValueError):
        path_str = "?"
    hint = f"{candidate[:4]}***" if candidate else "<missing>"
    _log("AUTH", f"gateway-auth 401 path={path_str} ip={ip} key={hint}")
    raise HTTPException(
        status_code=HTTP_401_UNAUTHORIZED,
        detail="Invalid API key. Provide a valid key via 'Authorization: Bearer <key>' or 'x-api-key: <key>'.",
    )


def extract_gateway_key_sync(request: object) -> Optional[str]:
    """Varian sync ringan untuk pemakaian non-Depends (opsional)."""
    try:
        return _extract_gateway_key(request) or None  # type: ignore[arg-type]
    except (AttributeError, TypeError, ValueError):
        return None

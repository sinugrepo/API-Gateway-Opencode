"""Route group: monitor."""
import asyncio
import json
import secrets
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, BackgroundTasks, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_502_BAD_GATEWAY,
    HTTP_503_SERVICE_UNAVAILABLE,
    HTTP_504_GATEWAY_TIMEOUT,
)
from app.core.config import LIVE_LOG_MAXLEN, MAX_RELAY_STREAM_ATTEMPTS, MODELS_CACHE_TTL_SECONDS, MONITOR_COOKIE_NAME, MONITOR_PASSWORD, MONITOR_TOKEN_TTL, RELAY_STREAM_BROKEN_COOLDOWN, SCAN_GUARD_BAN_SECONDS, SCAN_GUARD_ENABLED, SCAN_GUARD_THRESHOLD, SCAN_GUARD_TRUST_PROXY, SCAN_GUARD_WINDOW
from app.core.logging_utils import _get_live_logs, _live_log_lock, _live_logs, _log
from app.core import logging_utils as _logging_utils
from app.security.monitor_auth import _check_monitor, _make_monitor_token
from app.services.relay import _relay_penalty, _relay_penalty_lock, _relay_stream_broken, _relay_stream_broken_lock, test_relay_connection
from app.security.scan_guard import _banned_ips, _scan_guard_lock, _scan_guard_prune, _scan_hits
from app.services.usage import _query_usage_history, _resolve_period
from app.routes.misc import get_props, health
from app.routes.usage_routes import get_usage

router = APIRouter()

from pathlib import Path as _Path

_TMPL_DIR = _Path(__file__).resolve().parents[1] / "web" / "templates"


def _load_template(name: str) -> str:
    try:
        return (_TMPL_DIR / name).read_text(encoding="utf-8")
    except OSError:
        return ""


_MONITOR_LOGIN_PAGE = _load_template("login.html")
_MONITOR_DASHBOARD_PAGE = _load_template("dashboard.html")

@router.get("/monitor/login", response_class=HTMLResponse)
async def monitor_login_page(request: Request):
    if _check_monitor(request):
        return RedirectResponse(url="/monitor", status_code=302)
    return HTMLResponse(_MONITOR_LOGIN_PAGE)


@router.post("/monitor/login")
async def monitor_login(request: Request, password: str = Form(...)):
    if password != MONITOR_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    token = _make_monitor_token()
    redirect = RedirectResponse(url="/monitor", status_code=302)
    redirect.set_cookie(
        key=MONITOR_COOKIE_NAME,
        value=token,
        max_age=MONITOR_TOKEN_TTL,
        httponly=True,
        samesite="lax",
    )
    return redirect


@router.get("/monitor/logout")
async def monitor_logout():
    redirect = RedirectResponse(url="/monitor/login", status_code=302)
    redirect.delete_cookie(MONITOR_COOKIE_NAME)
    return redirect


@router.get("/monitor", response_class=HTMLResponse)
async def monitor_dashboard(request: Request):
    if not _check_monitor(request):
        return RedirectResponse(url="/monitor/login", status_code=302)
    return HTMLResponse(_MONITOR_DASHBOARD_PAGE)


@router.get("/monitor/api/health")
async def monitor_api_health(request: Request):
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    return await health()


@router.get("/monitor/api/usage")
async def monitor_api_usage(
    request: Request, period: str = "today",
    start: Optional[str] = None, end: Optional[str] = None,
):
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    return await get_usage(period=period, start=start, end=end)


@router.get("/monitor/api/usage/history")
async def monitor_api_usage_history(
    request: Request, period: str = "today",
    start: Optional[str] = None, end: Optional[str] = None,
):
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    _, start_time, end_time = _resolve_period(period, datetime.now(), start, end)
    buckets = await asyncio.to_thread(_query_usage_history, start_time, end_time)
    return {"period": period, "buckets": buckets}


@router.get("/monitor/api/relay")
async def monitor_api_relay(request: Request):
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    return await test_relay_connection()


@router.get("/monitor/api/props")
async def monitor_api_props(request: Request):
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    props = await get_props()
    extra = {
        "models_cache_ttl": MODELS_CACHE_TTL_SECONDS,
        "max_relay_stream_attempts": MAX_RELAY_STREAM_ATTEMPTS,
        "relay_stream_broken_cooldown": RELAY_STREAM_BROKEN_COOLDOWN,
    }
    return {**props.model_dump(), **extra}


@router.get("/monitor/api/security")
async def monitor_api_security(request: Request):
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    now = time.time()
    with _scan_guard_lock:
        _scan_guard_prune(now)
        banned = [
            {"ip": ip, "expires_in": max(0, int(until - now))}
            for ip, until in sorted(_banned_ips.items(), key=lambda kv: kv[1])
        ]
        watched = len(_scan_hits)
    return {
        "enabled": SCAN_GUARD_ENABLED,
        "threshold": SCAN_GUARD_THRESHOLD,
        "window_seconds": SCAN_GUARD_WINDOW,
        "ban_seconds": SCAN_GUARD_BAN_SECONDS,
        "trust_proxy": SCAN_GUARD_TRUST_PROXY,
        "banned_count": len(banned),
        "banned": banned,
        "watched_ips": watched,
    }


@router.post("/monitor/api/security/unban")
async def monitor_api_unban(request: Request):
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    try:
        body = await request.json()
    except (ValueError, TypeError):
        body = {}
    ip = (body.get("ip") or "").strip() if isinstance(body, dict) else ""
    if not ip:
        raise HTTPException(HTTP_400_BAD_REQUEST, "Field 'ip' is required")
    with _scan_guard_lock:
        removed_ban = _banned_ips.pop(ip, None) is not None
        removed_watch = _scan_hits.pop(ip, None) is not None
    return {"ip": ip, "unbanned": removed_ban or removed_watch}


@router.get("/monitor/api/logs")
async def monitor_api_logs(
    request: Request,
    since: int = 0,
    level: str = "",
    limit: int = 200,
):
    """Polling live-log: kembalikan entri dengan id > since.

    Ringan: membaca deque(maxlen=500) di bawah lock singkat, O(<=500).
    Dipakai dashboard untuk catch-up awal + fallback bila SSE terputus.
    """
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    logs, latest = _get_live_logs(since, level, limit)
    return {"logs": logs, "latest_id": latest, "maxlen": LIVE_LOG_MAXLEN}


@router.post("/monitor/api/logs/clear")
async def monitor_api_logs_clear(request: Request):
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    with _live_log_lock:
        _live_logs.clear()
        latest = _logging_utils._live_log_seq
    return {"cleared": True, "latest_id": latest}


@router.post("/monitor/api/relays/reset")
async def monitor_api_relays_reset(request: Request):
    """Hapus status circuit-breaker relay (429-cooldown + stream-broken).

    Wajib dipakai setelah redeploy relay (mis. setelah fix early-SSE
    di core.js): tanda stream-broken 1800s dari deployment LAMA akan terus
    mem-skip relay yang sebenarnya sudah sehat sampai cooldown habis.
    Non-streaming tidak terpengaruh (tidak pernah di-skip).
    """
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    with _relay_penalty_lock:
        penalty_cleared = len(_relay_penalty)
        _relay_penalty.clear()
    with _relay_stream_broken_lock:
        broken_cleared = len(_relay_stream_broken)
        _relay_stream_broken.clear()
    _log(
        "RELAY",
        f"relay state direset via monitor "
        f"(429-cooldown={penalty_cleared}, stream-broken={broken_cleared})",
    )
    return {
        "reset": True,
        "penalty_cleared": penalty_cleared,
        "stream_broken_cleared": broken_cleared,
    }


@router.get("/monitor/api/logs/stream")
async def monitor_api_logs_stream(request: Request, since: int = 0, level: str = ""):
    """SSE live-log: push entri baru tiap ~1s + heartbeat comment tiap 15s.

    Satu koneksi ringan per viewer; tidak ada broadcast fan-out mahal —
    tiap koneksi hanya polling deque sendiri. Heartbeat comment membuat
    reverse proxy/CDN tidak menutup koneksi idle.
    """
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    wanted = (level or "").strip().upper()

    async def _gen():
        last_id = since
        # Catch-up awal agar viewer langsung melihat konteks terakhir.
        init, latest = _get_live_logs(last_id, wanted, 200)
        for entry in init:
            yield f"data: {json.dumps(entry, ensure_ascii=False)}\n\n"
        last_id = latest
        idle_ticks = 0
        try:
            while True:
                # Batal bila klien putus.
                if await request.is_disconnected():
                    break
                fresh, latest = _get_live_logs(last_id, wanted, 200)
                for entry in fresh:
                    yield f"data: {json.dumps(entry, ensure_ascii=False)}\n\n"
                if fresh:
                    last_id = latest
                    idle_ticks = 0
                else:
                    idle_ticks += 1
                    # Heartbeat tiap ~15s agar proxy tidak kill koneksi idle.
                    if idle_ticks % 15 == 0:
                        yield ":\n\n"
                await asyncio.sleep(1.0)
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception:
            return

    return StreamingResponse(
        _gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

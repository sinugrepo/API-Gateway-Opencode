"""Route group: monitor."""
import asyncio
import json
import secrets
import threading
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
from app.core.errors import UpstreamEmptyResponse, UpstreamError
from app.core.logging_utils import _get_live_logs, _live_log_lock, _live_logs, _log
from app.core import logging_utils as _logging_utils
from app.security.monitor_auth import _check_monitor, _make_monitor_token
from app.services.relay import _relay_penalty, _relay_penalty_lock, _relay_stream_broken, _relay_stream_broken_lock, test_relay_connection
from app.security.scan_guard import _banned_ips, _scan_guard_lock, _scan_guard_prune, _scan_hits
from app.services.usage import _query_recent_requests, _query_usage_history, _resolve_period
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
    # Ikuti `next` bila diberikan login page (hindari open-redirect).
    nxt = (request.query_params.get("next") or "").strip()
    target = "/monitor"
    if nxt.startswith("/monitor"):
        target = nxt
    redirect = RedirectResponse(url=target, status_code=302)
    # Secure hanya bila dilewati HTTPS (hormati reverse proxy).
    forwarded_proto = (request.headers.get("x-forwarded-proto") or "").split(",")[0].strip().lower()
    is_secure = request.url.scheme == "https" or forwarded_proto == "https"
    redirect.set_cookie(
        key=MONITOR_COOKIE_NAME,
        value=token,
        max_age=MONITOR_TOKEN_TTL,
        httponly=True,
        samesite="lax",
        secure=is_secure,
        path="/",
    )
    return redirect


@router.get("/monitor/api/session")
async def monitor_api_session(request: Request):
    """Sisa TTL sesi monitor (untuk countdown di header dashboard)."""
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    token = request.cookies.get(MONITOR_COOKIE_NAME, "")
    ttl = MONITOR_TOKEN_TTL
    try:
        expiry = int(token.split(":")[0])
        import time as _t
        ttl = max(0, int(expiry - _t.time()))
    except (IndexError, ValueError):
        pass
    return {"ttl_seconds": ttl, "max_age": MONITOR_TOKEN_TTL}


@router.get("/monitor/logout")
async def monitor_logout():
    redirect = RedirectResponse(url="/monitor/login", status_code=302)
    redirect.delete_cookie(MONITOR_COOKIE_NAME, path="/")
    return redirect


@router.get("/monitor", response_class=HTMLResponse)
async def monitor_dashboard(request: Request):
    if not _check_monitor(request):
        return RedirectResponse(url="/monitor/login?expired=1", status_code=302)
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


@router.get("/monitor/api/requests/recent")
async def monitor_api_recent_requests(request: Request, limit: int = 20):
    """Daftar request terakhir: model + token in/out + timestamp.

    Dipakai panel Recent Requests di dashboard. Tidak ada filter period —
    selalu N terakhir agar operator melihat aktivitas terkini.
    `limit` dijepit 1..100 agar satu request tak bisa menarik seluruh tabel.
    """
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    limit = max(1, min(limit, 100))
    rows = await asyncio.to_thread(_query_recent_requests, limit)
    return {"requests": rows, "count": len(rows)}


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


# ==================== MODEL TEST (dashboard) ====================
# Uji tiap model satu-per-satu dari web tanpa curl: kirim prompt mungil
# via pipeline chat yang sama (bridge otomatis untuk Responses-only),
# non-stream, lalu kembalikan output + latency + usage.
# Dijalankan sekuensial dari UI (Test All loop satu-per-satu) agar tidak
# membombardir upstream dan memicu 429 massal.

_MODEL_TEST_DEFAULT_PROMPT = "jawab tepat satu kata: pong"
_MODEL_TEST_MAX_PROMPT_CHARS = 2000
_MODEL_TEST_MAX_TOKENS_LIMIT = 512
_MODEL_TEST_TIMEOUT_SECONDS = 180.0

# Rate-limit khusus endpoint uji manual: Test All (9 model) yang diklik
# dua kali dalam 5 menit = 18 hit. Batas 30 hit / 300 dtk meloloskan
# pemakaian wajar tapi menghentikan loop tak disengaja / script nyasar
# yang menembak endpoint ini berkala (kasus: batch uji misterius tiap
# ~15 menit di Recent Requests). Jendela geser in-memory per-proses.
_MODEL_TEST_RATE_LIMIT = 30
_MODEL_TEST_RATE_WINDOW_SECONDS = 300.0
_model_test_hits: List[float] = []
_model_test_hits_lock = threading.Lock()
# NOTE: batas per-proses; untuk deploy workers>1 tiap worker punya
# jendela sendiri — dokumentasikan, bukan bug untuk deploy default.


@router.get("/monitor/api/models")
async def monitor_api_models(request: Request):
    """Daftar model free + context window untuk dropdown/tabel Model Test."""
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    try:
        from app.services.models_cache import _fetch_opencode_free_models
        models = await _fetch_opencode_free_models()
        data = [m.model_dump() for m in models]
    except UpstreamError as exc:
        raise HTTPException(exc.status_code, str(exc.message))
    return {"models": data, "count": len(data)}


@router.post("/monitor/api/models/test")
async def monitor_api_model_test(request: Request):
    """Uji satu model dengan prompt mungil (non-stream).

    Body: {"model": "...", "prompt": "...", "max_tokens": 128}.
    Return: {model, ok, status, latency_ms, output, usage, error}.
    Selalu 200 di sisi monitor (ok=false bila upstream gagal) agar UI
    Test-All tidak berhenti di model pertama yang 429.
    """
    if not _check_monitor(request):
        raise HTTPException(status_code=401)
    try:
        body = await request.json()
    except (ValueError, TypeError):
        body = {}
    if not isinstance(body, dict):
        body = {}
    model = str(body.get("model") or "").strip()
    prompt = body.get("prompt", _MODEL_TEST_DEFAULT_PROMPT)
    prompt = prompt if isinstance(prompt, str) else str(prompt)
    try:
        max_tokens = int(body.get("max_tokens", 128))
    except (TypeError, ValueError):
        max_tokens = 128
    if not model:
        return {"model": "", "ok": False, "status": 400,
                "latency_ms": 0, "output": "", "usage": None,
                "error": "Field 'model' is required"}
    prompt = prompt.strip()[:_MODEL_TEST_MAX_PROMPT_CHARS] or _MODEL_TEST_DEFAULT_PROMPT
    max_tokens = max(1, min(max_tokens, _MODEL_TEST_MAX_TOKENS_LIMIT))

    # Rate-limit manual-test: tolak loop tak disengaja sebelum menyentuh
    # upstream. Selalu 200 + ok=false agar Test-All UI menandai LIMIT
    # per-baris, bukan crash.
    now = time.time()
    with _model_test_hits_lock:
        _model_test_hits[:] = [
            t for t in _model_test_hits
            if now - t < _MODEL_TEST_RATE_WINDOW_SECONDS
        ]
        if len(_model_test_hits) >= _MODEL_TEST_RATE_LIMIT:
            oldest = min(_model_test_hits)
            retry_after = max(1, int(_MODEL_TEST_RATE_WINDOW_SECONDS - (now - oldest)))
        else:
            retry_after = 0
            _model_test_hits.append(now)
    if retry_after:
        try:
            client_ip = request.client.host if request.client else "?"
        except Exception:
            client_ip = "?"
        _log("MODELS", f"model-test RATE-LIMITED model={model} ip={client_ip} retry_after={retry_after}s")
        return {"model": model, "ok": False, "status": 429,
                "latency_ms": 0, "output": "", "usage": None,
                "error": f"Too many manual tests, retry after {retry_after}s"}

    from types import SimpleNamespace
    from fastapi import BackgroundTasks as _BT
    from app.core.schemas import ChatCompletionRequest, ChatMessage
    from app.routes.chat import chat_completions

    req = ChatCompletionRequest(
        model=model,
        messages=[ChatMessage(role="user", content=prompt)],
        max_tokens=max_tokens,
        stream=False,
    )
    fake_request = SimpleNamespace(headers={})
    background = _BT()
    try:
        client_ip = request.client.host if request.client else "?"
    except Exception:
        client_ip = "?"
    started = time.time()
    try:
        result = await asyncio.wait_for(
            chat_completions(req, background, fake_request),
            timeout=_MODEL_TEST_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        _log("MODELS", f"model-test TIMEOUT model={model} ip={client_ip}")
        return {"model": model, "ok": False, "status": 504,
                "latency_ms": int((time.time() - started) * 1000),
                "output": "", "usage": None,
                "error": f"Timeout after {int(_MODEL_TEST_TIMEOUT_SECONDS)}s"}
    except UpstreamEmptyResponse:
        # Upstream hidup tapi nol konten (mis. max_tokens habis untuk
        # reasoning): model OK, output kosong — samakan kontrak bridge
        # non-stream chat (200 + content "").
        try:
            await background()
        except Exception:
            pass
        return {"model": model, "ok": True, "status": 200,
                "latency_ms": int((time.time() - started) * 1000),
                "output": "", "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                "error": None}
    except UpstreamError as exc:
        _log("MODELS", f"model-test FAIL model={model} ip={client_ip} status={exc.upstream_status or exc.status_code} err={exc.message[:120]}")
        return {"model": model, "ok": False,
                "status": exc.upstream_status or exc.status_code,
                "latency_ms": int((time.time() - started) * 1000),
                "output": "", "usage": None, "error": exc.message[:500]}
    except HTTPException as exc:
        return {"model": model, "ok": False, "status": exc.status_code,
                "latency_ms": int((time.time() - started) * 1000),
                "output": "", "usage": None,
                "error": str(exc.detail)[:500]}
    except Exception as exc:  # noqa: BLE001 — tampilkan ke UI, jangan 500
        return {"model": model, "ok": False, "status": 502,
                "latency_ms": int((time.time() - started) * 1000),
                "output": "", "usage": None, "error": f"{type(exc).__name__}: {exc}"[:500]}

    latency_ms = int((time.time() - started) * 1000)
    # StreamingResponse tak diharapkan (req.stream=False), tangani defensif.
    if not isinstance(result, dict):
        return {"model": model, "ok": False, "status": 502,
                "latency_ms": latency_ms, "output": "", "usage": None,
                "error": f"Unexpected result type {type(result).__name__}"}
    # Jalankan background tasks pipeline (pencatatan usage SQLite) yang
    # normalnya dieksekusi FastAPI setelah respons — bila dilewati, traffic
    # uji tak tercatat di statistik. Kegagalan catat tak boleh merusak hasil.
    try:
        await background()
    except Exception:
        pass
    try:
        choice = (result.get("choices") or [{}])[0]
        message = choice.get("message") or {}
        output = message.get("content")
        if isinstance(output, list):  # parts -> gabung teks
            texts = []
            for part in output:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    texts.append(part["text"])
                elif isinstance(part, str):
                    texts.append(part)
            output = "".join(texts)
        output = output if isinstance(output, str) else json.dumps(output or "", ensure_ascii=False)
    except (AttributeError, TypeError, ValueError):
        output = ""
    _log("MODELS", f"model-test OK model={model} ip={client_ip} latency={latency_ms}ms")
    return {"model": model, "ok": True, "status": 200,
            "latency_ms": latency_ms, "output": output[:2000],
            "usage": result.get("usage"), "error": None}

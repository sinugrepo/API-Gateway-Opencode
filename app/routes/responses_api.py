"""Route group: responses_api."""
import asyncio
import json
import secrets
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from app.security.gateway_auth import verify_gateway_key
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_502_BAD_GATEWAY,
    HTTP_503_SERVICE_UNAVAILABLE,
    HTTP_504_GATEWAY_TIMEOUT,
)
from app.core.config import API_KEY, MODEL, OPENCODE_RESPONSES_URL, RESPONSES_REVERSE_BRIDGE, STREAM_BYPASS_RELAY, USE_RELAY
from app.core.logging_utils import _log
from app.core.errors import UpstreamEmptyResponse, UpstreamError
from app.services.chat_bridge import (
    build_chat_request_from_responses,
    chat_completion_to_responses,
    chat_stream_to_responses_stream,
    empty_responses_object,
)
from app.services.model_endpoints import is_responses_native
from app.services.collect import collect_responses_object
from app.services.opencode import (
    _resolve_opencode_headers,
    _stable_opencode_session,
    ensure_responses_wire_fields,
)
from app.services.responses_bridge import (
    _extract_responses_usage,
    responses_stream_generator,
)
from app.services.usage import _safe_record

router = APIRouter()

async def create_response_via_chat(
    body: Dict[str, Any],
    client_model: str,
    background_tasks: BackgroundTasks,
    request: Request,
    stream: bool,
):
    """Layani request Responses untuk model yang native-nya BUKAN Responses.

    Model chat/messages-native (mimo, deepseek, glm, kimi, minimax, claude,
    qwen, ...) dijawab 500 oleh upstream bila dipaksa lewat Responses API
    (terbukti live). Fungsi ini menerjemahkan body ke request chat lalu
    memakai pipeline chat yang SAMA (rotasi relay, fingerprint, usage) dan
    membentuk hasilnya kembali menjadi objek/SSE Responses — dinamis per
    kategori endpoint, tanpa daftar pengecualian di route.
    """
    from app.routes.chat import chat_completions

    _log(
        "RESP",
        f"REVERSE-BRIDGE model={client_model} stream={stream} "
        f"-> chat pipeline (native endpoint bukan Responses)",
    )
    use_relay_req = body.pop("use_relay", None)
    chat_req = build_chat_request_from_responses(body, client_model)
    chat_req.stream = stream
    if use_relay_req is not None:
        chat_req.use_relay = bool(use_relay_req)
    if stream:
        if not API_KEY:
            raise UpstreamError(
                "OPENCODE_API_KEY is not configured",
                status_code=HTTP_503_SERVICE_UNAVAILABLE,
            )
        chat_resp = await chat_completions(chat_req, background_tasks, request)
        if not isinstance(chat_resp, StreamingResponse):
            # Defensif: seharusnya selalu StreamingResponse bila stream=True.
            return JSONResponse(
                content=chat_completion_to_responses(chat_resp, client_model)
                if isinstance(chat_resp, dict)
                else empty_responses_object(client_model),
                status_code=200,
            )
        return StreamingResponse(
            chat_stream_to_responses_stream(
                chat_resp.body_iterator, client_model=client_model,
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
    try:
        result = await chat_completions(chat_req, background_tasks, request)
    except UpstreamEmptyResponse:
        # Samakan kontrak bridge non-stream: upstream kosong tetap 200
        # dengan output kosong (bukan 502) agar klien bisa retry sendiri.
        return JSONResponse(content=empty_responses_object(client_model), status_code=200)
    if not isinstance(result, dict):
        return JSONResponse(content=empty_responses_object(client_model), status_code=200)
    # Usage sudah dicatat pipeline chat via background_tasks — jangan catat
    # ganda di sini.
    return JSONResponse(content=chat_completion_to_responses(result, client_model), status_code=200)


@router.post("/v1/responses", dependencies=[Depends(verify_gateway_key)])
@router.post("/responses", dependencies=[Depends(verify_gateway_key)])
async def create_response(request: Request, background_tasks: BackgroundTasks):
    """OpenAI Responses API pass-through (untuk Muse Spark & model sejenis).

    Body diteruskan mentah ke upstream (relay round-robin + fallback direct),
    karena skema Responses (`input`, `reasoning`, dsb.) berbeda dari chat dan
    tidak perlu dinormalisasi. Field khusus proxy `use_relay` dicabut sebelum
    diteruskan agar upstream tidak 400 karena field tak dikenal.

    Kontrak routing: endpoint ini TIDAK PERNAH memakai bridge
    responses_to_chat_stream_generator — bridge hanya untuk /v1/chat/completions
    dengan model Responses-only. Di sini selalu responses_stream_generator
    (pass-through mentah, termasuk event reasoning apa adanya).
    """
    route_start = time.time()
    try:
        body = await request.json()
    except (ValueError, TypeError, json.JSONDecodeError):
        raise HTTPException(HTTP_400_BAD_REQUEST, "Invalid JSON body")
    if not isinstance(body, dict):
        raise HTTPException(HTTP_400_BAD_REQUEST, "JSON body must be an object")

    client_model = (body.get("model") or MODEL or "").strip()
    if not client_model:
        raise HTTPException(
            HTTP_400_BAD_REQUEST,
            "The model field is required; choose one from GET /v1/models",
        )

    # Routing dinamis per kategori endpoint native (model_endpoints):
    # model yang native-nya BUKAN Responses (chat/messages) TIDAK diteruskan
    # mentah ke OPENCODE_RESPONSES_URL (upstream menjawab 500) melainkan
    # dijembatani balik lewat pipeline chat.
    if not is_responses_native(client_model):
        if not RESPONSES_REVERSE_BRIDGE:
            raise HTTPException(
                HTTP_400_BAD_REQUEST,
                f"Model '{client_model}' is served via /v1/chat/completions, "
                f"not /v1/responses",
            )
        # Identitas CLI + sesi stabil diurus pipeline chat sendiri
        # (chat_completions me-resolve dari request + messages hasil konversi).
        return await create_response_via_chat(
            body, client_model, background_tasks, request,
            bool(body.get("stream", False)),
        )

    use_relay_req = body.pop("use_relay", None)
    stream_use_relay = use_relay_req if use_relay_req is not None else USE_RELAY
    if STREAM_BYPASS_RELAY:
        stream_use_relay = False
    # Free-tier fingerprint gate (403 bila hilang, diverifikasi live
    # 2026-09-18): kuartet tools + store=false + max_output_tokens.
    # Berlaku untuk stream MAUPUN non-stream.
    ensure_responses_wire_fields(body)
    stream = bool(body.get("stream", False))
    # Identitas CLI untuk free tier (dibagi ke semua upstream attempt).
    # Sesi dibuat STABIL per-percakapan (bukan acak per-request): konten
    # reasoning `encrypted_content` yang direplay klien stateless di-turn
    # berikutnya di-issuance ke caller identity turn pertama. Sesi acak baru
    # tiap request membuat upstream menolaknya (400 "encrypted_content was
    # not issued to this caller") dan percakapan brick permanen.
    oc_headers = _resolve_opencode_headers(request.headers)
    # _resolve_opencode_headers mengisi sesi acak bila klien tidak mengirim
    # x-opencode-session; ganti dengan sesi stabil per-percakapan.
    client_sent_session = bool(
        (request.headers.get("x-opencode-session") or "").strip()
    )
    if not client_sent_session:
        oc_headers["x-opencode-session"] = _stable_opencode_session(body)

    if stream:
        if not API_KEY:
            raise UpstreamError(
                "OPENCODE_API_KEY is not configured",
                status_code=HTTP_503_SERVICE_UNAVAILABLE,
            )
        return StreamingResponse(
            responses_stream_generator(
                body,
                client_model=client_model,
                background_tasks=background_tasks,
                use_relay=bool(stream_use_relay),
                opencode_headers=oc_headers,
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # Non-stream via buffered stream: upstream free tier HANYA menerima
    # stream:true (403 bila tidak). Generator pass-through dijalankan
    # internal dengan wire stream:true (rotasi relay + auto-heal tetap
    # jalan) lalu event response.completed di-buffer menjadi satu objek.
    wire_body = dict(body)
    wire_body["stream"] = True
    result = await collect_responses_object(
        lambda: responses_stream_generator(
            wire_body,
            client_model=client_model,
            background_tasks=background_tasks,
            use_relay=bool(stream_use_relay),
            opencode_headers=oc_headers,
        ),
        client_model=client_model,
    )

    if isinstance(result, dict):
        usage = _extract_responses_usage(result)
        if usage:
            try:
                route_duration_ms = int((time.time() - route_start) * 1000)
            except (TypeError, ValueError):
                route_duration_ms = 0
            background_tasks.add_task(
                _safe_record,
                request_id=str(result.get("id", f"resp-{secrets.token_hex(8)}")),
                model=client_model,
                prompt_tokens=usage.get("prompt_tokens", 0),
                completion_tokens=usage.get("completion_tokens", 0),
                total_tokens=usage.get("total_tokens", 0),
                duration_ms=max(0, route_duration_ms),
            )
        return JSONResponse(content=result, status_code=200)
    raise UpstreamError(
        "Invalid Responses object from upstream",
        status_code=HTTP_502_BAD_GATEWAY,
    )

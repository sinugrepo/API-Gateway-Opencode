"""Route group: responses_api."""
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
from app.core.config import API_KEY, MODEL, OPENCODE_RESPONSES_URL, RATE_LIMIT_BACKOFF, STREAM_BYPASS_RELAY, USE_RELAY
from app.core.logging_utils import _log
from app.core.errors import UpstreamError
from app.services.opencode import _resolve_opencode_headers, _stable_opencode_session
from app.services.responses_bridge import (
    _extract_responses_usage,
    _is_encrypted_content_rejection,
    _strip_replayed_reasoning,
    responses_stream_generator,
)
from app.services.upstream import _retry_after_seconds, call_upstream
from app.services.usage import _safe_record

router = APIRouter()

@router.post("/v1/responses")
@router.post("/responses")
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

    use_relay_req = body.pop("use_relay", None)
    stream_use_relay = use_relay_req if use_relay_req is not None else USE_RELAY
    if STREAM_BYPASS_RELAY:
        stream_use_relay = False
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

    response, _ = await call_upstream(
        body,
        stream=False,
        use_relay=bool(stream_use_relay),
        target_url=OPENCODE_RESPONSES_URL,
        extra_headers=oc_headers,
    )

    # AUTO-HEAL (non-stream): penolakan replay encrypted_content tidak
    # sembuh dengan rotasi target; buang reasoning replay lalu coba SEKALI.
    if (
        response.status_code == 400
        and _is_encrypted_content_rejection(response.text[:500])
    ):
        healed_body, removed = _strip_replayed_reasoning(body)
        if removed:
            _log(
                "RESP",
                f"HEAL non-stream | encrypted_content ditolak -> buang "
                f"{removed} item reasoning replay, retry 1x",
            )
            response, _ = await call_upstream(
                healed_body,
                stream=False,
                use_relay=bool(stream_use_relay),
                target_url=OPENCODE_RESPONSES_URL,
                extra_headers=oc_headers,
            )

    if response.status_code != 200:
        detail = response.text[:500]
        if response.status_code == 429:
            raise UpstreamError(
                f"Upstream rate limited (429): {detail}",
                status_code=HTTP_429_TOO_MANY_REQUESTS,
                upstream_status=429,
                retry_after=_retry_after_seconds(response, RATE_LIMIT_BACKOFF),
            )
        raise UpstreamError(
            f"Upstream responded with {response.status_code}: {detail}",
            status_code=HTTP_502_BAD_GATEWAY,
            upstream_status=response.status_code,
        )

    try:
        result = response.json()
    except (ValueError, TypeError, json.JSONDecodeError):
        return Response(
            content=response.content,
            media_type="application/json",
            status_code=200,
        )

    if isinstance(result, dict):
        usage = _extract_responses_usage(result)
        if usage:
            background_tasks.add_task(
                _safe_record,
                request_id=str(result.get("id", f"resp-{secrets.token_hex(8)}")),
                model=client_model,
                prompt_tokens=usage.get("prompt_tokens", 0),
                completion_tokens=usage.get("completion_tokens", 0),
                total_tokens=usage.get("total_tokens", 0),
            )
        return JSONResponse(content=result, status_code=200)
    return Response(
        content=response.content,
        media_type="application/json",
        status_code=200,
    )

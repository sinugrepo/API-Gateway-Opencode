"""Route group: chat."""
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
from app.core.config import API_KEY, OPENCODE_RESPONSES_URL, STREAM_BYPASS_RELAY, USE_RELAY, _is_responses_only_model
from app.core.errors import UpstreamEmptyResponse, UpstreamError
from app.core.logging_utils import _log
from app.services.collect import collect_chat_completion
from app.services.opencode import _resolve_opencode_headers, _stable_opencode_session
from app.services.responses_bridge import (
    build_responses_payload_from_chat,
    responses_to_chat_stream_generator,
)
from app.core.schemas import ChatCompletionRequest
from app.services.streaming import stream_generator
from app.services.upstream import _resolve_request_model, build_upstream_payload

router = APIRouter()

async def chat_completions_via_responses(
    req: ChatCompletionRequest,
    client_model: str,
    background_tasks: BackgroundTasks,
    opencode_headers: Optional[Dict[str, str]] = None,
):
    """Layani chat request untuk model Responses-only via Responses API.

    Non-stream: Responses upstream HANYA menerima `stream:true` (free-tier
    gate, 403 bila tidak) sehingga generator bridge dijalankan internal
    dengan wire `stream:true` lalu hasilnya di-buffer menjadi satu
    chat completion. Stream: terjemahkan SSE Responses ke SSE chat.
    """
    use_relay = req.use_relay if req.use_relay is not None else USE_RELAY
    responses_payload = build_responses_payload_from_chat(req)
    _log(
        "RESP",
        f"CHAT-BRIDGE model={client_model} stream={req.stream} "
        f"input_items={len(responses_payload.get('input', []))}",
    )

    if req.stream:
        if not API_KEY:
            raise UpstreamError(
                "OPENCODE_API_KEY is not configured",
                status_code=HTTP_503_SERVICE_UNAVAILABLE,
            )
        stream_use_relay = use_relay
        if STREAM_BYPASS_RELAY:
            stream_use_relay = False
        include_usage_requested = bool(
            isinstance(req.stream_options, dict)
            and req.stream_options.get("include_usage")
        )
        return StreamingResponse(
            responses_to_chat_stream_generator(
                responses_payload,
                client_model=client_model,
                include_usage_requested=include_usage_requested,
                background_tasks=background_tasks,
                use_relay=bool(stream_use_relay),
                opencode_headers=opencode_headers,
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    wire_payload = dict(responses_payload)
    wire_payload["stream"] = True
    try:
        content, tool_calls, _finish, usage = await collect_chat_completion(
            lambda: responses_to_chat_stream_generator(
                wire_payload,
                client_model=client_model,
                include_usage_requested=True,
                background_tasks=background_tasks,
                use_relay=bool(use_relay),
                opencode_headers=opencode_headers,
            ),
            client_model=client_model,
        )
    except UpstreamEmptyResponse:
        # Kontrak lama bridge non-stream: upstream kosong tetap 200 dengan
        # content "" (bukan 502) agar klien bisa retry/fallback sendiri.
        content, tool_calls = "", []
        usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    message: Dict[str, Any] = {"role": "assistant", "content": content}
    if tool_calls:
        message["tool_calls"] = tool_calls

    return {
        "id": f"chatcmpl-{secrets.token_hex(16)}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": client_model,
        "choices": [
            {
                "index": 0,
                "message": message,
                "finish_reason": "tool_calls" if tool_calls else "stop",
            }
        ],
        "usage": {
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
            "total_tokens": usage.get("total_tokens", 0),
        },
    }


@router.post("/v1/chat/completions", dependencies=[Depends(verify_gateway_key)])
async def chat_completions(
    req: ChatCompletionRequest,
    background_tasks: BackgroundTasks,
    request: Request,
):
    if not req.messages:
        raise HTTPException(HTTP_400_BAD_REQUEST, "Messages cannot be empty")

    client_model = _resolve_request_model(req)
    # Identitas CLI untuk free tier (dibagi ke semua upstream attempt).
    # getattr agar tahan Request/Headers/dict/None.
    oc_headers = _resolve_opencode_headers(getattr(request, "headers", request))

    # Sesi STABIL per-percakapan bila klien tidak mengirim sendiri
    # x-opencode-session (`opencode-session.md` §7: 1 ID per conversation
    # untuk cache affinity; request ID tetap unik per POST dari resolver).
    # Berlaku untuk SEMUA model chat (bukan cuma bridge): sesi acak
    # per-request memutus affinity cache + membuat replay
    # `reasoning.encrypted_content` ditolak upstream pada turn berikutnya.
    if not (request.headers.get("x-opencode-session") or "").strip():
        oc_headers = dict(oc_headers)
        oc_headers["x-opencode-session"] = _stable_opencode_session(
            {"messages": [message.to_upstream() for message in req.messages]}
        )

    # Model Responses-only (muse-spark): klien chat-only seperti Hermes tidak
    # bisa diarahkan ke /v1/responses — jembatani otomatis di sini.
    if _is_responses_only_model(client_model):
        return await chat_completions_via_responses(
            req, client_model, background_tasks, oc_headers
        )

    payload = build_upstream_payload(req)

    if req.stream:
        if not API_KEY:
            raise UpstreamError(
                "OPENCODE_API_KEY is not configured",
                status_code=HTTP_503_SERVICE_UNAVAILABLE,
            )

        # Streaming kini menghormati use_relay per-request, sama seperti
        # path non-streaming. STREAM_BYPASS_RELAY hanya untuk debugging.
        stream_use_relay = req.use_relay if req.use_relay is not None else USE_RELAY
        if STREAM_BYPASS_RELAY:
            stream_use_relay = False

        include_usage_requested = bool(
            isinstance(req.stream_options, dict)
            and req.stream_options.get("include_usage")
        )
        return StreamingResponse(
            stream_generator(
                payload,
                client_model=client_model,
                include_usage_requested=include_usage_requested,
                background_tasks=background_tasks,
                use_relay=stream_use_relay,
                opencode_headers=oc_headers,
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    use_relay = req.use_relay if req.use_relay is not None else USE_RELAY
    # Free-tier gate: upstream HANYA menerima stream:true (403 bila tidak).
    # Jalankan generator streaming internal dengan wire stream:true lalu
    # buffer menjadi satu chat completion untuk klien non-stream.
    wire_payload = dict(payload)
    wire_payload["stream"] = True
    wire_stream_opts = wire_payload.get("stream_options")
    if not isinstance(wire_stream_opts, dict):
        wire_stream_opts = {}
    wire_stream_opts["include_usage"] = True
    wire_payload["stream_options"] = wire_stream_opts
    content, tool_calls, finish_reason, usage = await collect_chat_completion(
        lambda: stream_generator(
            wire_payload,
            client_model=client_model,
            include_usage_requested=True,
            background_tasks=background_tasks,
            use_relay=use_relay,
            opencode_headers=oc_headers,
        ),
        client_model=client_model,
    )

    message: Dict[str, Any] = {"role": "assistant", "content": content}
    if tool_calls:
        message["tool_calls"] = tool_calls

    return {
        "id": f"chatcmpl-{secrets.token_hex(16)}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": client_model,
        "choices": [
            {
                "index": 0,
                "message": message,
                "finish_reason": finish_reason,
            }
        ],
        "usage": usage,
    }

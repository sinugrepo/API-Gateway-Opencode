"""Route group: chat."""
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
from app.core.config import API_KEY, OPENCODE_RESPONSES_URL, RATE_LIMIT_BACKOFF, STREAM_BYPASS_RELAY, USE_RELAY, _is_responses_only_model
from app.core.errors import UpstreamError
from app.core.logging_utils import _log
from app.services.opencode import _resolve_opencode_headers, _stable_opencode_session
from app.services.responses_bridge import (
    _extract_responses_usage,
    _is_encrypted_content_rejection,
    _responses_output_to_chat,
    _strip_replayed_reasoning,
    build_responses_payload_from_chat,
    responses_to_chat_stream_generator,
)
from app.core.schemas import ChatCompletionRequest
from app.services.streaming import stream_generator
from app.services.tools_dsml import extract_response_content
from app.services.upstream import _resolve_request_model, _retry_after_seconds, build_upstream_payload, call_upstream
from app.services.usage import _safe_record

router = APIRouter()

async def chat_completions_via_responses(
    req: ChatCompletionRequest,
    client_model: str,
    background_tasks: BackgroundTasks,
    opencode_headers: Optional[Dict[str, str]] = None,
):
    """Layani chat request untuk model Responses-only via Responses API.

    Non-stream: panggil Responses upstream, konversi hasilnya ke chat
    completion. Stream: terjemahkan SSE Responses ke SSE chat.
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

    response, _ = await call_upstream(
        responses_payload,
        stream=False,
        use_relay=bool(use_relay),
        target_url=OPENCODE_RESPONSES_URL,
        extra_headers=opencode_headers,
    )

    # AUTO-HEAL (bridge non-stream): penolakan replay encrypted_content
    # tidak sembuh dengan rotasi target; buang reasoning replay lalu SEKALI.
    if (
        response.status_code == 400
        and _is_encrypted_content_rejection(response.text[:500])
    ):
        healed_payload, removed = _strip_replayed_reasoning(responses_payload)
        if removed:
            _log(
                "RESP",
                f"HEAL bridge non-stream | encrypted_content ditolak -> buang "
                f"{removed} item reasoning replay, retry 1x",
            )
            response, _ = await call_upstream(
                healed_payload,
                stream=False,
                use_relay=bool(use_relay),
                target_url=OPENCODE_RESPONSES_URL,
                extra_headers=opencode_headers,
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
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise UpstreamError(
            f"Invalid JSON from upstream: {exc}",
            status_code=HTTP_502_BAD_GATEWAY,
        ) from exc

    if not isinstance(result, dict):
        raise UpstreamError(
            "Invalid Responses object from upstream",
            status_code=HTTP_502_BAD_GATEWAY,
        )

    content, tool_calls = _responses_output_to_chat(result.get("output"))
    usage = _extract_responses_usage(result) or {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }

    background_tasks.add_task(
        _safe_record,
        request_id=str(result.get("id", f"chatcmpl-{secrets.token_hex(16)}")),
        model=client_model,
        prompt_tokens=usage.get("prompt_tokens", 0),
        completion_tokens=usage.get("completion_tokens", 0),
        total_tokens=usage.get("total_tokens", 0),
    )

    message: Dict[str, Any] = {"role": "assistant", "content": content}
    if tool_calls:
        message["tool_calls"] = tool_calls

    return {
        "id": result.get("id", f"chatcmpl-{secrets.token_hex(16)}"),
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


@router.post("/v1/chat/completions")
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
    response, _ = await call_upstream(
        payload, stream=False, use_relay=use_relay, extra_headers=oc_headers
    )

    if response.status_code != 200:
        # Keep only a bounded diagnostic. Do not expose keys or full HTML.
        detail = response.text[:500]
        if response.status_code == 429:
            # Jaring pengaman: call_upstream biasanya sudah melempar 429 yang
            # bersih. Kalau response 429 lolos sampai sini, jangan samarkan
            # menjadi 502 — teruskan 429 + Retry-After ke klien.
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
    except json.JSONDecodeError as exc:
        raise UpstreamError(
            f"Invalid JSON from upstream: {exc}",
            status_code=HTTP_502_BAD_GATEWAY,
        ) from exc

    content, tool_calls, finish_reason = extract_response_content(result)

    message: Dict[str, Any] = {"role": "assistant", "content": content}
    if tool_calls:
        message["tool_calls"] = tool_calls

    usage = result.get("usage", {}) or {}

    # Record usage as a fire-and-forget task via FastAPI BackgroundTasks so
    # the insert runs after the response is sent. This survives across
    # requests instead of being lost when the wrapper task is finalised
    # before the thread worker dispatches. Streaming requests are
    # intentionally not tracked here: their `usage` payload is consumed by
    # the browser/agent and only includes token counts when the client
    # forwards `stream_options.include_usage`, which this proxy does not.
    background_tasks.add_task(
        _safe_record,
        request_id=result.get("id", f"chatcmpl-{secrets.token_hex(16)}"),
        model=client_model,
        prompt_tokens=usage.get("prompt_tokens", 0),
        completion_tokens=usage.get("completion_tokens", 0),
        total_tokens=usage.get("total_tokens", 0),
    )

    return {
        "id": result.get("id", f"chatcmpl-{secrets.token_hex(16)}"),
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
        "system_fingerprint": result.get("system_fingerprint"),
    }

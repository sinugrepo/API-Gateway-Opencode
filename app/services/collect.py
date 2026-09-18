"""Non-streaming via upstream streaming (free-tier gate).

Upstream Zen free tier menolak SEMUA request non-streaming dengan 403
FreeTierError (diverifikasi live 2026-09-18): `stream:false` maupun tanpa
`stream` ditolak walau fingerprint lain valid — untuk /chat/completions
MAUPUN /v1/responses. Karena itu path non-stream klien tidak lagi POST
`stream:false` ke upstream; sebagai gantinya generator streaming yang sudah
ada (rotasi relay, 429-cooldown, auto-heal encrypted_content, vision-direct)
dijalankan internal lalu hasilnya di-buffer menjadi satu respons JSON.

Reuse ini disengaja: seluruh logika failover tetap satu implementasi.
"""
import json
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from fastapi import BackgroundTasks
from starlette.status import HTTP_502_BAD_GATEWAY

from app.core.config import RATE_LIMIT_BACKOFF
from app.core.errors import UpstreamEmptyResponse, UpstreamError


def _iter_sse_data(raw: Any) -> List[str]:
    """Pecah satu yield generator menjadi daftar payload `data:`."""
    if not isinstance(raw, str) or not raw:
        return []
    out: List[str] = []
    for line in raw.splitlines():
        if not line or line.startswith(":"):
            continue
        if line.startswith("data:"):
            out.append(line[5:].lstrip())
    return out


def _raise_for_error_chunk(err: Any) -> None:
    """Petakan error-chunk SSE internal menjadi exception route."""
    detail = ""
    code = ""
    retry_after: Optional[float] = None
    if isinstance(err, dict):
        raw_detail = err.get("detail", err.get("message", ""))
        detail = str(raw_detail)[:300] if raw_detail is not None else ""
        code = str(err.get("code", ""))
        try:
            retry_after = float(err.get("retry_after")) if err.get("retry_after") is not None else None
        except (TypeError, ValueError):
            retry_after = None
    if code == "RATE_LIMITED":
        raise UpstreamError(
            f"Upstream rate limited (429): {detail}",
            status_code=429,
            upstream_status=429,
            retry_after=retry_after if retry_after and retry_after > 0 else RATE_LIMIT_BACKOFF,
        )
    if code == "EMPTY_RESPONSE":
        raise UpstreamEmptyResponse("Upstream returned an empty response")
    raise UpstreamError(
        f"Upstream request failed: {detail or code or 'unknown error'}",
        status_code=HTTP_502_BAD_GATEWAY,
    )


async def collect_chat_completion(
    stream_factory,
    *,
    client_model: str,
) -> Tuple[str, List[Dict[str, Any]], str, Dict[str, int]]:
    """Buffer SSE chat (OpenAI chunk) menjadi (content, tool_calls, finish, usage).

    `stream_factory` adalah callable tanpa argumen yang mengembalikan async
    iterator SSE dari `stream_generator` /
    `responses_to_chat_stream_generator` (sudah di-wire dengan payload
    stream:true, relay, headers, background_tasks oleh pemanggil).
    """
    content_parts: List[str] = []
    calls: Dict[int, Dict[str, Any]] = {}
    finish_reason: Optional[str] = None
    usage: Optional[Dict[str, Any]] = None
    pending_error: Any = None

    gen = stream_factory() if callable(stream_factory) else stream_factory
    async for raw in gen:
        for data in _iter_sse_data(raw):
            if data == "[DONE]":
                break
            try:
                obj = json.loads(data)
            except (ValueError, TypeError):
                continue
            if not isinstance(obj, dict):
                continue
            if isinstance(obj.get("error"), dict):
                pending_error = obj["error"]
                continue
            if isinstance(obj.get("usage"), dict) and not obj.get("choices"):
                usage = obj["usage"]
                continue
            choices = obj.get("choices")
            if not isinstance(choices, list) or not choices:
                continue
            choice = choices[0] if isinstance(choices[0], dict) else {}
            if choice.get("finish_reason") is not None:
                finish_reason = str(choice["finish_reason"])
            chunk_usage = obj.get("usage")
            if isinstance(chunk_usage, dict) and chunk_usage:
                usage = chunk_usage
            delta = choice.get("delta") or {}
            if not isinstance(delta, dict):
                continue
            text = delta.get("content")
            if isinstance(text, str) and text:
                content_parts.append(text)
            fragments = delta.get("tool_calls")
            if not isinstance(fragments, list):
                continue
            for pos, frag in enumerate(fragments):
                if not isinstance(frag, dict):
                    continue
                try:
                    index = int(frag.get("index", pos))
                except (TypeError, ValueError):
                    index = pos
                entry = calls.setdefault(
                    index,
                    {"id": None, "type": "function", "name": None, "arguments": ""},
                )
                if frag.get("id") and not entry["id"]:
                    entry["id"] = str(frag["id"])
                if isinstance(frag.get("type"), str) and frag["type"]:
                    entry["type"] = frag["type"]
                function = frag.get("function")
                if isinstance(function, dict):
                    if isinstance(function.get("name"), str) and function["name"] and not entry["name"]:
                        entry["name"] = function["name"]
                    args = function.get("arguments")
                    if args is not None:
                        entry["arguments"] += args if isinstance(args, str) else str(args)

    if pending_error is not None:
        _raise_for_error_chunk(pending_error)

    content = "".join(content_parts)
    tool_calls: List[Dict[str, Any]] = []
    for index in sorted(calls):
        entry = calls[index]
        arguments = entry["arguments"] or "{}"
        tool_calls.append({
            "id": entry["id"] or f"call_{index}",
            "type": "function",
            "function": {
                "name": entry["name"] or "",
                "arguments": arguments,
            },
        })
    # Entri tanpa nama bukan tool-call valid (fragmen yatim); buang agar
    # tidak dikirim ke klien sebagai call buntu.
    tool_calls = [c for c in tool_calls if c["function"]["name"]]

    if not content and not tool_calls:
        raise UpstreamEmptyResponse("No visible content or tool calls in upstream response")

    finish = "tool_calls" if tool_calls else (finish_reason or "stop")
    final_usage = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }
    if isinstance(usage, dict):
        try:
            final_usage = {
                "prompt_tokens": int(usage.get("prompt_tokens", 0) or 0),
                "completion_tokens": int(usage.get("completion_tokens", 0) or 0),
                "total_tokens": int(usage.get("total_tokens", 0) or 0),
            }
        except (TypeError, ValueError):
            pass
    return content, tool_calls, str(finish), final_usage


async def collect_responses_object(
    stream_factory,
    *,
    client_model: str,
) -> Dict[str, Any]:
    """Buffer SSE Responses mentah menjadi satu objek response final.

    Mengutamakan event `response.completed` / `response.incomplete` /
    `response.failed` (membawa objek response penuh), lalu objek buffered
    utuh, terakhir rakitan minimal dari delta teks bila stream terpotong.
    """
    final: Optional[Dict[str, Any]] = None
    text_parts: List[str] = []
    usage: Optional[Dict[str, int]] = None
    pending_error: Any = None
    response_id = ""

    gen = stream_factory() if callable(stream_factory) else stream_factory
    async for raw in gen:
        for data in _iter_sse_data(raw):
            if data == "[DONE]":
                break
            try:
                obj = json.loads(data)
            except (ValueError, TypeError):
                continue
            if not isinstance(obj, dict):
                continue
            if isinstance(obj.get("error"), dict):
                pending_error = obj["error"]
                continue
            # Objek buffered utuh (relay membungkus satu JSON penuh).
            if obj.get("object") == "response" and isinstance(obj.get("output"), list):
                final = obj
                continue
            event_type = str(obj.get("type", ""))
            if event_type in ("response.completed", "response.incomplete", "response.failed"):
                inner = obj.get("response")
                if isinstance(inner, dict):
                    final = inner
                continue
            if event_type in ("response.output_text.delta", "response.text.delta"):
                delta = obj.get("delta", "")
                if delta is None:
                    continue
                text = delta if isinstance(delta, str) else str(delta)
                if text:
                    text_parts.append(text)
                continue
            if event_type == "response.created":
                inner = obj.get("response") or {}
                if isinstance(inner, dict) and inner.get("id"):
                    response_id = str(inner["id"])
                continue
            inner_resp = obj.get("response")
            if isinstance(inner_resp, dict) and isinstance(inner_resp.get("usage"), dict):
                try:
                    from app.services.responses_bridge import _extract_responses_usage
                    found = _extract_responses_usage({"usage": inner_resp["usage"]})
                    if found:
                        usage = found
                except (ImportError, AttributeError, TypeError, ValueError):
                    pass

    if final is not None:
        return final
    if pending_error is not None:
        _raise_for_error_chunk(pending_error)

    text = "".join(text_parts)
    if not text:
        raise UpstreamEmptyResponse("Upstream returned an empty response")
    return {
        "id": response_id or f"resp-{client_model}-{int(time.time())}",
        "object": "response",
        "created_at": int(time.time()),
        "status": "incomplete",
        "model": client_model,
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text}],
        }],
        "usage": {
            "input_tokens": (usage or {}).get("prompt_tokens", 0),
            "output_tokens": (usage or {}).get("completion_tokens", 0),
            "total_tokens": (usage or {}).get("total_tokens", 0),
        },
    }

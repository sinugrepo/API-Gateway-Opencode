"""Responses API pass-through + chat<->Responses bridge."""
import asyncio
import json
import secrets
import time
from contextlib import suppress
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Union

import httpx
from fastapi import BackgroundTasks, HTTPException
from starlette.status import HTTP_503_SERVICE_UNAVAILABLE

from app.core.config import (
    API_KEY,
    BRIDGE_REQUEST_TIMEOUT,
    DIRECT_FIRST_SLOW,
    FORBIDDEN_FRESH_SESSION_RETRY,
    FORBIDDEN_RETRY_DELAY,
    HERMES_COMPAT,
    HERMES_TOOL_INSTRUCTION,
    MODEL,
    OPENCODE_RESPONSES_URL,
    RATE_LIMIT_BACKOFF,
    RATE_LIMIT_COOLDOWN,
    REASONING_FORWARD,
    RELAY_403_COOLDOWN,
    RELAY_FALLBACK,
    RELAY_STREAM_BROKEN_COOLDOWN,
    SSE_KEEPALIVE_INTERVAL,
    USE_RELAY,
    _is_responses_only_model,
)
from app.core.errors import TimeoutError_, UpstreamEmptyResponse, UpstreamError
from app.core.http_client import _get_http
from app.core.logging_utils import _log
from app.services.opencode import _fresh_request_headers, _fresh_retry_targets, _oc_session_tag
from app.services.relay import (
    _is_giant_payload,
    _is_relay_timeout,
    _limit_stream_targets,
    _mark_relay_forbidden,
    _mark_relay_rate_limited,
    _mark_relay_stream_broken,
    _order_targets_direct_first,
    _payload_has_media,
    _relay_batch_for_request,
    _should_mark_stream_broken,
    _stream_request_headers,
    _with_relay_headers,
)
from app.core.schemas import ChatCompletionRequest
from app.core.sse import _sse
from app.services.tools_dsml import clean_visible_text
from app.services.usage import _safe_record
from app.services.upstream import (
    _classify_rate_limit,
    _relay_cooldown_seconds,
    _retry_after_seconds,
    _should_retry_same_route_429,
    call_upstream,
)


def _extract_responses_usage(obj: Any) -> Optional[Dict[str, int]]:
    """Ambil token usage dari objek Responses API secara best-effort.

    Format Responses: {"input_tokens":..,"output_tokens":..,"total_tokens":..},
    kadang bersarang di response.usage. Selalu kembalikan mapping gaya chat
    (prompt/completion/total) atau None. Tidak pernah melempar.
    """
    try:
        if not isinstance(obj, dict):
            return None
        usage = obj.get("usage")
        if not isinstance(usage, dict):
            resp = obj.get("response")
            if isinstance(resp, dict) and isinstance(resp.get("usage"), dict):
                usage = resp["usage"]
            else:
                return None
        prompt = usage.get("input_tokens", usage.get("prompt_tokens", 0)) or 0
        completion = usage.get("output_tokens", usage.get("completion_tokens", 0)) or 0
        total = usage.get("total_tokens", 0) or (prompt + completion)
        return {
            "prompt_tokens": int(prompt),
            "completion_tokens": int(completion),
            "total_tokens": int(total),
        }
    except (TypeError, ValueError, AttributeError):
        return None


def _chat_text_content(content: Any) -> str:
    """Ambil teks dari chat content (string | list parts | None)."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for part in content:
            if isinstance(part, dict):
                if part.get("type") in ("text", "input_text", "output_text"):
                    text = part.get("text", "")
                    parts.append(text if isinstance(text, str) else str(text))
            elif isinstance(part, str):
                parts.append(part)
        return "".join(parts)
    return str(content)


# Batas total byte media (gambar + file) per request bridge.
# Relay menolak body >4.5MB; gagal-cepat 400 yang jelas lebih baik daripada
# 413/504 misterius, dan biner raksasa hanya membakar kuota giant-payload.
MAX_BRIDGE_IMAGE_BYTES = 3_000_000
MAX_BRIDGE_MEDIA_BYTES = MAX_BRIDGE_IMAGE_BYTES


def _image_byte_size(url: str) -> int:
    """Estimasi byte gambar dari data-URL base64; 0 untuk URL biasa."""
    try:
        head, _, b64 = url.partition(",")
        if b64 and ";base64" in head:
            return len(b64) * 3 // 4
    except (TypeError, ValueError, AttributeError):
        pass
    return 0


def _media_byte_size(url_or_data: str) -> int:
    """Estimasi byte media dari data-URL base64; 0 untuk URL/file_id biasa."""
    try:
        head, _, b64 = url_or_data.partition(",")
        if b64 and ";base64" in head:
            return len(b64) * 3 // 4
    except (TypeError, ValueError, AttributeError):
        pass
    return 0


def _image_byte_size(url: str) -> int:
    """Estimasi byte gambar (alias kompatibel dari penghitung media)."""
    return _media_byte_size(url)


def _guess_media_ext(data_url: str, default: str) -> str:
    """Tebak ekstensi dari mime data-URL untuk filename cadangan."""
    try:
        head = data_url.split(",", 1)[0].lower()
        if "application/pdf" in head:
            return ".pdf"
        if "image/png" in head:
            return ".png"
        if "image/jpeg" in head or "image/jpg" in head:
            return ".jpg"
        if "image/webp" in head:
            return ".webp"
        if "image/gif" in head:
            return ".gif"
        if "text/plain" in head:
            return ".txt"
    except (TypeError, AttributeError, IndexError):
        pass
    return default


def _anthropic_source_to_data_url(source: Any) -> str:
    """Ubah Anthropic `source` menjadi data-URL (`""` bila tak dikenal).

    Bentuk yang didukung (Kilo/Cline/Roo kadang mengirim gaya Anthropic
    walau lewat endpoint OpenAI-compatible):
    - `{"type": "base64", "media_type": "image/png", "data": "..."}`
    - `{"type": "url", "url": "..."}`
    Tidak pernah melempar.
    """
    try:
        if not isinstance(source, dict):
            return ""
        stype = source.get("type")
        if stype == "base64":
            data = source.get("data")
            if not isinstance(data, str) or not data:
                return ""
            mime = source.get("media_type") or "image/png"
            if not isinstance(mime, str) or "/" not in mime:
                mime = "image/png"
            if data.startswith("data:"):
                return data
            return f"data:{mime};base64,{data}"
        if stype == "url":
            url = source.get("url")
            return url if isinstance(url, str) and url else ""
    except (TypeError, ValueError, AttributeError):
        pass
    return ""


def _chat_media_contents(content: Any) -> List[Dict[str, Any]]:
    """Ekstrak SEMUA part media gaya chat menjadi part Responses.

    - `{"type": "image_url", ...}` -> `{"type": "input_image", ...}`
    - `{"type": "file", "file": {"file_data", "filename"}}` (PDF/dokumen)
      -> `{"type": "input_file", ...}`
    - `{"type": "file", "file": {"file_id": ...}}` -> `input_file` by id.
    - `{"type": "image", "source": {...}}` (gaya Anthropic: base64/url)
      -> `{"type": "input_image", ...}` (Kilo/Cline/Roo kadang mengirim ini).
    - `{"type": "input_image"/"input_file", ...}` (gaya Responses nyasar via
      chat) -> diteruskan apa adanya (dinormalisasi ringan).
    Budget byte (base64) dipakai BERSAMA gambar+file; lewat batas -> HTTP 400.
    """
    media: List[Dict[str, Any]] = []
    if not isinstance(content, list):
        return media
    total = 0

    def _charge(nbytes: int) -> None:
        nonlocal total
        total += nbytes
        if total > MAX_BRIDGE_MEDIA_BYTES:
            raise HTTPException(
                status_code=400,
                detail=f"Total media ~{total // 1024}KB melebihi batas "
                f"{MAX_BRIDGE_MEDIA_BYTES // 1000}KB per request "
                f"(relay menolak body >4.5MB; kecilkan/kompres file)",
            )

    for part in content:
        if not isinstance(part, dict):
            continue
        ptype = part.get("type")
        if ptype == "image_url":
            ref = part.get("image_url")
            url, detail = None, "auto"
            if isinstance(ref, dict):
                url = ref.get("url")
                if ref.get("detail") in ("auto", "low", "high"):
                    detail = ref["detail"]
            elif isinstance(ref, str):
                url = ref
            if not url or not isinstance(url, str):
                continue
            _charge(_media_byte_size(url))
            media.append({"type": "input_image", "image_url": url, "detail": detail})
        elif ptype == "file":
            ref = part.get("file")
            if not isinstance(ref, dict):
                continue
            if ref.get("file_id") and isinstance(ref["file_id"], str):
                media.append({"type": "input_file", "file_id": ref["file_id"]})
                continue
            data = ref.get("file_data")
            if not data or not isinstance(data, str):
                continue
            _charge(_media_byte_size(data))
            filename = ref.get("filename")
            if not filename or not isinstance(filename, str):
                filename = "file" + _guess_media_ext(data, ".bin")
            media.append({"type": "input_file", "filename": filename,
                          "file_data": data})
        elif ptype in ("image", "input_image"):
            # Gaya Anthropic (`source`) atau Responses nyasar via chat.
            # Prioritas: source Anthropic -> image_url str/dict -> file_id.
            url = ""
            detail = part.get("detail") if part.get("detail") in ("auto", "low", "high") else "auto"
            source_url = _anthropic_source_to_data_url(part.get("source"))
            if source_url:
                url = source_url
            else:
                ref = part.get("image_url")
                if isinstance(ref, dict):
                    if isinstance(ref.get("url"), str) and ref["url"]:
                        url = ref["url"]
                    if ref.get("detail") in ("auto", "low", "high"):
                        detail = ref["detail"]
                elif isinstance(ref, str) and ref:
                    url = ref
            if url:
                _charge(_media_byte_size(url))
                media.append({"type": "input_image", "image_url": url, "detail": detail})
                continue
            file_id = part.get("file_id")
            if isinstance(file_id, str) and file_id:
                media.append({"type": "input_image", "file_id": file_id})
                continue
            # `image` tanpa payload yang dikenal -> lewati (bukan crash).
            continue
        elif ptype == "input_file":
            # Passthrough Responses-style nyasar via chat.
            file_id = part.get("file_id")
            if isinstance(file_id, str) and file_id:
                media.append({"type": "input_file", "file_id": file_id})
                continue
            data = part.get("file_data")
            if isinstance(data, str) and data:
                _charge(_media_byte_size(data))
                filename = part.get("filename")
                if not isinstance(filename, str) or not filename:
                    filename = "file" + _guess_media_ext(data, ".bin")
                media.append({"type": "input_file", "filename": filename,
                              "file_data": data})
                continue
            continue
        elif isinstance(part.get("image_url"), (str, dict)):
            # Toleransi: part gambar tanpa type eksplisit.
            ref = part["image_url"]
            url = ref.get("url") if isinstance(ref, dict) else ref
            if not url or not isinstance(url, str):
                continue
            _charge(_media_byte_size(url))
            media.append({"type": "input_image", "image_url": url, "detail": "auto"})
    return media


def _chat_image_contents(content: Any) -> List[Dict[str, Any]]:
    """Hanya part gambar (subset _chat_media_contents; budget tetap berbagi)."""
    return [p for p in _chat_media_contents(content) if p.get("type") == "input_image"]


def _chat_messages_to_responses_input(
    messages: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Konversi pesan chat OpenAI menjadi array `input` Responses API."""
    items: List[Dict[str, Any]] = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        role = message.get("role", "user")
        text = _chat_text_content(message.get("content"))
        media = _chat_media_contents(message.get("content"))
        if role == "tool":
            items.append(
                {
                    "type": "function_call_output",
                    "call_id": message.get("tool_call_id") or "",
                    "output": text,
                }
            )
            continue
        tool_calls = message.get("tool_calls")
        if role == "assistant" and isinstance(tool_calls, list) and tool_calls:
            if text:
                items.append(
                    {
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": text}, *media],
                    }
                )
            for call in tool_calls:
                if not isinstance(call, dict):
                    continue
                function = call.get("function") or {}
                if not isinstance(function, dict):
                    continue
                arguments = function.get("arguments", "{}")
                if isinstance(arguments, (dict, list)):
                    arguments = json.dumps(arguments, ensure_ascii=False)
                elif arguments is None:
                    arguments = "{}"
                items.append(
                    {
                        "type": "function_call",
                        "call_id": call.get("id") or "",
                        "name": function.get("name") or "",
                        "arguments": str(arguments),
                    }
                )
            continue
        if role == "system":
            items.append(
                {"role": "system", "content": [{"type": "input_text", "text": text}, *media]}
            )
            continue
        if role not in ("user", "assistant", "developer"):
            role = "user"
        part_type = "output_text" if role == "assistant" else "input_text"
        items.append(
            {"role": role, "content": [{"type": part_type, "text": text}, *media]}
        )
    return items


def _chat_tools_to_responses_tools(
    tools: Optional[List[Dict[str, Any]]],
) -> Optional[List[Dict[str, Any]]]:
    """Konversi chat tools -> Responses tools (keduanya skema function)."""
    if not tools:
        return None
    converted: List[Dict[str, Any]] = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        function = tool.get("function")
        if isinstance(function, dict):
            entry: Dict[str, Any] = {"type": "function"}
            if function.get("name"):
                entry["name"] = function["name"]
            if function.get("description"):
                entry["description"] = function["description"]
            if function.get("parameters") is not None:
                entry["parameters"] = function["parameters"]
            if entry.get("name"):
                converted.append(entry)
        elif tool.get("name"):
            entry = {"type": "function", "name": tool["name"]}
            if tool.get("description"):
                entry["description"] = tool["description"]
            if tool.get("parameters") is not None:
                entry["parameters"] = tool["parameters"]
            converted.append(entry)
    return converted or None


def _chat_tool_choice_to_responses(
    tool_choice: Optional[Union[str, Dict[str, Any]]],
) -> Optional[Union[str, Dict[str, Any]]]:
    """Konversi chat tool_choice -> Responses tool_choice."""
    if tool_choice is None:
        return None
    if isinstance(tool_choice, str):
        return tool_choice
    if isinstance(tool_choice, dict):
        if tool_choice.get("type") == "function":
            inner = tool_choice.get("function")
            if isinstance(inner, dict) and inner.get("name"):
                return {"type": "function", "name": inner["name"]}
            if tool_choice.get("name"):
                return {"type": "function", "name": tool_choice["name"]}
        return None
    return None


def build_responses_payload_from_chat(req: ChatCompletionRequest) -> Dict[str, Any]:
    """Bangun body Responses API dari request chat (untuk model bridge).

    Diselaraskan dengan wire /v1/responses yang terbukti lolos free-tier
    (log 2026-09-18 14:00:45: keys input/max_output_tokens/model/
    prompt_cache_key/store/stream/tool_choice/tools -> 200 OK 18s):

    - stream:true WAJIB (gate 403; jalur bridge-streaming lama tidak
      mengirimnya sehingga Hermes/Kilo via /chat selalu 403 relay+direct).
    - store:false + kuartet tools fingerprint + max_output_tokens WAJIB.
    - tool_choice:"auto" + prompt_cache_key stabil disintesis bila klien
      chat tidak mengirimnya (klien Responses seperti Kilo selalu kirim).
    - muse-spark: reasoning effort SELALU xhigh (override klien).
    """
    messages = [message.to_upstream() for message in req.messages]
    if HERMES_COMPAT and req.tools:
        messages = [
            {"role": "system", "content": HERMES_TOOL_INSTRUCTION},
            *messages,
        ]
    payload: Dict[str, Any] = {
        "model": (req.model or MODEL or "").strip(),
        "input": _chat_messages_to_responses_input(messages),
    }
    if req.temperature is not None:
        payload["temperature"] = req.temperature
    if req.top_p is not None:
        payload["top_p"] = req.top_p
    if req.max_tokens is not None:
        payload["max_output_tokens"] = req.max_tokens
    else:
        # Klien chat boleh mengirim max_tokens:null; upstream 403 bila
        # max_output_tokens hilang (bagian dari fingerprint gate).
        payload["max_output_tokens"] = 65536
    responses_tools = _chat_tools_to_responses_tools(req.tools)
    if responses_tools is not None:
        payload["tools"] = responses_tools
    responses_choice = _chat_tool_choice_to_responses(req.tool_choice)
    if responses_choice is not None:
        payload["tool_choice"] = responses_choice
    if req.reasoning_effort:
        payload["reasoning"] = {"effort": req.reasoning_effort}
    # Free-tier fingerprint gate (403 bila hilang, diverifikasi live
    # 2026-09-18): stream:true + kuartet tools + store=false +
    # max_output_tokens + tool_choice + prompt_cache_key.
    payload["stream"] = True
    payload["store"] = False
    try:
        from app.services.opencode import (
            coerce_tool_choice_auto,
            ensure_responses_fingerprint_tools,
            ensure_spark_reasoning_xhigh,
        )
        ensure_responses_fingerprint_tools(payload)
        # muse-spark: reasoning effort SELALU xhigh (override nilai klien).
        ensure_spark_reasoning_xhigh(payload)
        # Provider Console HANYA mendukung tool_choice "auto" (live
        # 2026-09-24: named/required/none dari Kilo -> 400 di semua target).
        # Bila tools ada paksa "auto"; bila tidak ada, drop key-nya sekalian.
        coerce_tool_choice_auto(payload, "chat-bridge")
    except (ImportError, AttributeError, TypeError):
        # Fallback minimal bila helper tak tersedia: default auto seperti
        # sebelumnya (tanpanya upstream 403 bila tools ada).
        if payload.get("tool_choice") is None and payload.get("tools"):
            payload["tool_choice"] = "auto"
    # prompt_cache_key stabil per-percakapan (samakan dengan sukses direct).
    # Dipetakan dari fingerprint percakapan agar stabil antar-turn/restart.
    if not payload.get("prompt_cache_key"):
        try:
            from app.services.opencode import _conversation_fingerprint
            fingerprint = _conversation_fingerprint({"messages": messages})
            if fingerprint:
                payload["prompt_cache_key"] = fingerprint[:32]
        except (ImportError, AttributeError, TypeError, ValueError):
            pass
    return payload


def _responses_output_to_chat(
    output: Any,
) -> Tuple[str, List[Dict[str, Any]]]:
    """Konversi array `output` Responses -> (content, tool_calls) gaya chat."""
    texts: List[str] = []
    tool_calls: List[Dict[str, Any]] = []
    if not isinstance(output, list):
        return "", []
    for item in output:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type == "message":
            for part in item.get("content") or []:
                if not isinstance(part, dict):
                    continue
                if part.get("type") in ("output_text", "text", "refusal"):
                    text = part.get("text", "")
                    if text:
                        texts.append(text if isinstance(text, str) else str(text))
        elif item_type == "function_call":
            arguments = item.get("arguments", "{}")
            if isinstance(arguments, (dict, list)):
                arguments = json.dumps(arguments, ensure_ascii=False)
            elif arguments is None:
                arguments = "{}"
            tool_calls.append(
                {
                    "id": item.get("call_id") or item.get("id") or f"call_{len(tool_calls)}",
                    "type": "function",
                    "function": {
                        "name": item.get("name") or "",
                        "arguments": str(arguments),
                    },
                }
            )
    return "".join(texts), tool_calls


def _extract_responses_reasoning_delta(event: Dict[str, Any]) -> str:
    """Ambil fragmen reasoning dari satu event SSE Responses (best-effort).

    Event reasoning upstream bervariasi antar versi:
    `response.reasoning_text.delta`, `response.reasoning_summary_text.delta`,
    `response.reasoning_summary_part.added`, dsb. Semuanya membawa string di
    `delta`/`text`/`summary_text`. Kembalikan "" bila bukan event reasoning.
    Tidak pernah melempar.
    """
    try:
        if not isinstance(event, dict):
            return ""
        event_type = str(event.get("type", ""))
        if "reasoning" not in event_type:
            return ""
        for key in ("delta", "text", "summary_text"):
            value = event.get(key)
            if isinstance(value, str) and value:
                return value
            if value is not None and not isinstance(value, (dict, list)):
                text = str(value)
                if text:
                    return text
        return ""
    except (TypeError, ValueError, AttributeError):
        return ""


def _extract_responses_reasoning_text(output: Any) -> str:
    """Ambil teks reasoning dari array `output` Responses penuh (buffered).

    Item reasoning berbentuk `{"type":"reasoning","summary":[{"type":
    "summary_text","text":"..."}]}` (varian: `content`, `text`). Dipakai
    sebagai fallback anti-empty saat output tidak punya message/function_call.
    """
    try:
        if not isinstance(output, list):
            return ""
        collected: List[str] = []

        def _collect_texts(parts: Any) -> None:
            if not isinstance(parts, list):
                return
            for part in parts:
                if not isinstance(part, dict):
                    continue
                for key in ("text", "summary_text"):
                    value = part.get(key)
                    if isinstance(value, str) and value:
                        collected.append(value)

        for item in output:
            if not isinstance(item, dict):
                continue
            if item.get("type") != "reasoning":
                continue
            _collect_texts(item.get("summary"))
            _collect_texts(item.get("content"))
            for key in ("text", "summary_text"):
                value = item.get(key)
                if isinstance(value, str) and value:
                    collected.append(value)
        return "".join(collected)
    except (TypeError, ValueError, AttributeError):
        return ""


def _count_item_kind(counts: Dict[str, int], phase: str, item: Any) -> None:
    """Hitung kemunculan item output per (fase, tipe): added:reasoning:1.

    Observability untuk kasus done-item tanpa konten terekstrak (EMPTY
    dengan wire besar). Bounded 32 kunci. Tidak pernah melempar.
    """
    try:
        if not isinstance(counts, dict):
            return
        kind = item.get("type") if isinstance(item, dict) else None
        key = f"{phase}:{kind if isinstance(kind, str) and kind else '?'}"
        if len(counts) < 32 or key in counts:
            counts[key] = counts.get(key, 0) + 1
    except (TypeError, ValueError, AttributeError):
        pass


def _format_event_types(counts: Any) -> str:
    """Render ringkas {'a':1} -> 'a:1' untuk log (observability).

    Dipakai di SUMMARY + EMPTY-STREAM agar kasus "banyak byte, nol konten"
    langsung terlihat komposisi event-nya (mis. hanya created + item.done
    reasoning + completed). Tidak pernah melempar.
    """
    try:
        if not isinstance(counts, dict) or not counts:
            return "-"
        items = sorted(counts.items(), key=lambda kv: str(kv[0]))[:32]
        return ",".join(f"{k}:{v}" for k, v in items)[:300]
    except (TypeError, ValueError, AttributeError):
        return "-"


def _payload_has_replay_reasoning(payload: Any) -> bool:
    """True bila payload input membawa item reasoning ber-encrypted_content."""
    try:
        if not isinstance(payload, dict):
            return False
        items = payload.get("input")
        if not isinstance(items, list):
            return False
        return any(
            isinstance(item, dict)
            and item.get("type") == "reasoning"
            and isinstance(item.get("encrypted_content"), str)
            and item["encrypted_content"]
            for item in items
        )
    except (TypeError, ValueError, AttributeError):
        return False


def _is_encrypted_content_rejection(detail: str) -> bool:
    """True bila detail error upstream = penolakan replay encrypted_content.

    Pesan persis: "reasoning `encrypted_content` was not issued to this
    caller". Konten terenkripsi di-issuance ke caller identity turn
    pertama; bila identitas berubah (atau percakapan dimulai sebelum fix
    sesi-stabil), SEMUA target menolak dengan 400 yang sama.
    """
    if not detail:
        return False
    lowered = detail.lower()
    return "encrypted_content" in lowered and (
        "not issued to this caller" in lowered or "was not issued" in lowered
    )


def _strip_replayed_reasoning(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
    """Buang item reasoning yang membawa encrypted_content dari payload input.

    Responses stateless (store:false + include reasoning.encrypted_content)
    mereplay item reasoning turn sebelumnya di `input`. Bila caller identity
    sudah tidak cocok, upstream menolak SELURUH request. Menghapus item
    reasoning itu membuat request diterima lagi (model kehilangan konten
    thinking lama, tapi teks/tool-call history tetap utuh).

    Return (payload_bersama, jumlah_item_dibuang). Payload asli TIDAK
    dimutasi (salinan dangkal + salinan daftar input).
    """
    if not isinstance(payload, dict):
        return payload, 0
    items = payload.get("input")
    if not isinstance(items, list):
        return payload, 0
    kept: List[Dict[str, Any]] = []
    removed = 0
    for item in items:
        if (
            isinstance(item, dict)
            and item.get("type") == "reasoning"
            and isinstance(item.get("encrypted_content"), str)
            and item["encrypted_content"]
        ):
            removed += 1
            continue
        kept.append(item)
    if not removed:
        return payload, 0
    healed = dict(payload)
    healed["input"] = kept
    return healed, removed


async def responses_stream_generator(
    payload: Dict[str, Any],
    *,
    client_model: str,
    background_tasks: BackgroundTasks,
    use_relay: bool,
    opencode_headers: Optional[Dict[str, str]] = None,
):
    """Teruskan SSE Responses API upstream ke klien mentah (pass-through).

    Beda dengan stream_generator chat: tidak ada parsing DSML/tool-call —
    setiap baris SSE diteruskan apa adanya. Failover antar-target hanya
    sebelum byte pertama sampai ke klien; setelah itu putus = error chunk.
    """
    stream_id = f"resp-{secrets.token_hex(8)}"
    stream_start = time.time()
    wire_bytes = 0
    data_events = 0
    sent_first_byte = False
    stream_completed = False
    last_usage: Optional[Dict[str, int]] = None
    response_id = payload.get("id") or stream_id

    def log_summary(note: str = "") -> None:
        try:
            _log(
                "RESP",
                f"SUMMARY id={stream_id} {note} model={client_model} "
                f"wire_bytes={wire_bytes} events={data_events} "
                f"total={time.time() - stream_start:.2f}s",
            )
        except Exception:  # noqa: BLE001 - diagnostik tak boleh crash
            pass

    base_headers = _stream_request_headers(opencode_headers)
    targets: List[Tuple[str, Dict[str, str]]] = []
    if use_relay:
        for relay_url in _relay_batch_for_request(for_stream=True):
            targets.append((relay_url, _with_relay_headers(OPENCODE_RESPONSES_URL, base_headers)))
        if RELAY_FALLBACK:
            targets.append((OPENCODE_RESPONSES_URL, dict(base_headers)))
    else:
        targets.append((OPENCODE_RESPONSES_URL, dict(base_headers)))
    targets = _limit_stream_targets(targets)
    vision_direct_first = use_relay and RELAY_FALLBACK and _payload_has_media(payload)
    if vision_direct_first:
        # Vision: direct DULU; relay HANYA fallback bila direct 429
        # (lihat guard di loop). Biner base64 rawan 413/504 relay.
        direct = [t for t in targets if "x-relay-target" not in t[1]]
        relays = [t for t in targets if "x-relay-target" in t[1]]
        targets = direct + relays
        _log("RESP", "VISION: direct dulu, relay khusus 429")
    if DIRECT_FIRST_SLOW and (
        _is_responses_only_model(client_model) or _is_giant_payload(payload)
    ):
        # Lihat STREAM/SLOW di stream_generator: relay untuk kasus ini pasti
        # 504 tanpa byte; direct dulu agar thinking tidak dibuang + klien
        # seperti Hermes tidak reconnect dalam loop stall.
        targets = _order_targets_direct_first(targets)
        _log("RESP", f"SLOW model={client_model}: direct dulu, relay fallback")

    last_error: Optional[str] = None
    last_rate_limited = False
    last_retry_after: Optional[float] = None
    # Bug spam-429 opencode khusus muse-spark: 1x same-route retry per target.
    spurious_429_retried: set = set()
    # AUTO-HEAL encrypted_content: sekali per request, kembali ke target 0.
    healed_once = False
    heal_restart = False
    # Pelacakan upaya terakhir fresh-session (lihat stream_generator chat
    # untuk rationale lengkap): hanya bila SETIAP kegagalan adalah 403
    # pra-byte-pertama dan TIDAK ADA kegagalan lain.
    forbidden_count = 0
    saw_non_403_failure = False
    fresh_session_retry_done = False
    _log("RESP", f"REQ model={client_model} stream_keys={sorted(payload.keys())} {_oc_session_tag(opencode_headers)}")

    try:
        # Fase luar (while True) hanya berputar SEKALI ekstra: fase
        # fresh-session sebagai upaya terakhir all-403 (lihat bawah).
        while True:
            target_index = 0
            while target_index < len(targets):
                target_url, headers = targets[target_index]
                is_relay = "x-relay-target" in headers
                if vision_direct_first and is_relay and not last_rate_limited:
                    # Relay vision hanya untuk 429 direct; kegagalan lain
                    # selesai di direct (last_error sudah terisi).
                    break
                # Request ID fresh per attempt ala CLI asli (msg_ unik per POST).
                headers = _fresh_request_headers(headers)
                _log(
                    "RESP",
                    f"ATTEMPT {target_index + 1}/{len(targets)} "
                    f"{'RELAY' if is_relay else 'DIRECT'} target={target_url}",
                )
                try:
                    client = _get_http()
                    async with client.stream(
                        "POST", target_url, json=payload, headers=headers
                    ) as response:
                        if response.status_code != 200:
                            try:
                                body = await response.aread()
                                detail = body.decode("utf-8", errors="replace")[:500]
                            except Exception:  # noqa: BLE001
                                detail = ""
                            # AUTO-HEAL: penolakan replay encrypted_content tidak
                            # akan sembuh dengan rotasi target (payload sama, semua
                            # target menolak). Buang item reasoning replay lalu
                            # mulai lagi dari target pertama, SEKALI per request.
                            if (
                                response.status_code == 400
                                and not sent_first_byte
                                and not healed_once
                                and _is_encrypted_content_rejection(detail)
                                and _payload_has_replay_reasoning(payload)
                            ):
                                payload, removed = _strip_replayed_reasoning(payload)
                                if removed:
                                    healed_once = True
                                    last_error = (
                                        f"Upstream responded with {response.status_code}: {detail}"
                                    )
                                    _log(
                                        "RESP",
                                        f"HEAL {target_url} | encrypted_content ditolak "
                                        f"-> buang {removed} item reasoning replay, "
                                        f"retry dari target pertama",
                                    )
                                    target_index = 0
                                    continue
                            if is_relay and _is_relay_timeout(response):
                                # Konteks raksasa -> 504 Edge wajar (TTFB>25s),
                                # jangan tandai relay sehat sebagai broken.
                                if _should_mark_stream_broken(target_url, payload):
                                    _mark_relay_stream_broken(
                                        target_url,
                                        time.time() + RELAY_STREAM_BROKEN_COOLDOWN,
                                    )
                                last_error = f"Relay stream timeout (504): {detail[:200]}"
                                saw_non_403_failure = True
                                target_index += 1
                                _log(
                                    "RESP",
                                    f"STREAM-TIMEOUT {target_url} | Vercel kill 504 "
                                    f"(limit eksekusi ~25s, bukan bug proxy) -> relay "
                                    f"di-skip streaming {RELAY_STREAM_BROKEN_COOLDOWN:.0f}s, "
                                    f"lanjut ke target berikutnya/direct",
                                )
                                continue
                            if response.status_code == 429 and not sent_first_byte:
                                # PENGECUALIAN bug spam-429 muse-spark: retry 1x
                                # same-route dulu sebelum rotasi/cooldown.
                                if _should_retry_same_route_429(client_model) and target_url not in spurious_429_retried:
                                    spurious_429_retried.add(target_url)
                                    _delay = _retry_after_seconds(response, RATE_LIMIT_BACKOFF)
                                    _log(
                                        "RESP",
                                        f"SPURIOUS-429 {target_url} | retry same-route 1x "
                                        f"in {_delay:.1f}s sebelum ganti route",
                                    )
                                    await asyncio.sleep(_delay)
                                    continue  # ulangi target_index yang sama
                                rate_cls = _classify_rate_limit(response)
                                last_retry_after = _retry_after_seconds(
                                    response, RATE_LIMIT_BACKOFF
                                )
                                _mark_relay_rate_limited(
                                    target_url,
                                    time.time() + _relay_cooldown_seconds(response),
                                )
                                last_error = f"Rate limited (429): {detail}"
                                last_rate_limited = True
                                saw_non_403_failure = True
                                _log(
                                    "RESP",
                                    f"RATE-LIMITED {target_url} | "
                                    f"{rate_cls['description']} | detail={detail[:300]!r}",
                                )
                                target_index += 1
                                continue
                            last_error = (
                                f"Upstream responded with {response.status_code}: {detail}"
                            )
                            last_rate_limited = response.status_code == 429
                            if response.status_code == 403:
                                # Dihitung untuk relay MAUPUN direct (lihat
                                # stream_generator: 403 direct = kandidat
                                # fresh-session retry).
                                forbidden_count += 1
                            else:
                                saw_non_403_failure = True
                            if response.status_code == 403 and is_relay and not sent_first_byte:
                                _mark_relay_forbidden(
                                    target_url,
                                    time.time() + RELAY_403_COOLDOWN,
                                )
                                _log(
                                    "RESP",
                                    f"FORBIDDEN {target_url} | upstream 403 "
                                    f"(IP relay di-flag, bukan salah fingerprint) "
                                    f"-> relay di-cooldown {RELAY_403_COOLDOWN:.0f}s",
                                )
                            target_index += 1
                            _log(
                                "RESP",
                                f"FAIL {target_url} | upstream-status={response.status_code} "
                                f"detail={detail[:300]!r}",
                            )
                            continue

                        _log("RESP", f"OK {target_url}")
                        relay_target_failed = False
                        line_iter = response.aiter_lines()
                        pending_line_task: Optional[asyncio.Task[str]] = None
                        last_activity_at = time.time()
                        try:
                            while True:
                                # Samakan dengan bridge (BRIDGE_REQUEST_TIMEOUT):
                                # endpoint Responses melayani model yang sama dengan
                                # TTFB wajar 120-300 dtk pada konteks panjang.
                                # REQUEST_TIMEOUT global (120s) membunuh stream sehat.
                                idle_seconds = time.time() - last_activity_at
                                if idle_seconds > BRIDGE_REQUEST_TIMEOUT:
                                    raise asyncio.TimeoutError(
                                        f"No upstream progress for {int(idle_seconds)}s"
                                        f" (idle timeout {BRIDGE_REQUEST_TIMEOUT:.0f}s)"
                                    )
                                if pending_line_task is None:
                                    pending_line_task = asyncio.create_task(
                                        line_iter.__anext__()
                                    )
                                done, _ = await asyncio.wait(
                                    (pending_line_task,),
                                    timeout=min(
                                        SSE_KEEPALIVE_INTERVAL,
                                        max(0.0, BRIDGE_REQUEST_TIMEOUT - idle_seconds),
                                    ),
                                    return_when=asyncio.FIRST_COMPLETED,
                                )
                                if not done:
                                    yield ":\n\n"
                                    continue
                                try:
                                    line = pending_line_task.result()
                                except StopAsyncIteration:
                                    stream_completed = True
                                    break
                                finally:
                                    pending_line_task = None

                                if not line:
                                    yield "\n"
                                    continue
                                if line.startswith(":"):
                                    yield f"{line}\n\n"
                                    continue
                                if not line.startswith("data:"):
                                    continue
                                data = line[5:].lstrip()
                                wire_bytes += len(data)
                                last_activity_at = time.time()
                                if data == "[DONE]":
                                    sent_first_byte = True
                                    stream_completed = True
                                    break
                                # Relay early-SSE: fetch upstream gagal dilaporkan
                                # sebagai event (HTTP sudah 200). Cegat SEBELUM
                                # menandai sent_first_byte agar failover ke target
                                # berikutnya tetap jalan.
                                if '"relay.error"' in data and not sent_first_byte:
                                    try:
                                        _err_obj = json.loads(data)
                                    except (ValueError, TypeError):
                                        _err_obj = None
                                    if isinstance(_err_obj, dict) and _err_obj.get("type") == "relay.error":
                                        _relay_status = _err_obj.get("status")
                                        _relay_body = str(_err_obj.get("body") or "")[:300]
                                        if _relay_status == 429:
                                            last_retry_after = RATE_LIMIT_BACKOFF
                                            last_rate_limited = True
                                            saw_non_403_failure = True
                                            _mark_relay_rate_limited(
                                                target_url,
                                                time.time() + RATE_LIMIT_COOLDOWN,
                                            )
                                        elif _relay_status == 403:
                                            last_rate_limited = False
                                            forbidden_count += 1
                                            _mark_relay_forbidden(
                                                target_url,
                                                time.time() + RELAY_403_COOLDOWN,
                                            )
                                        else:
                                            last_rate_limited = _relay_status == 429
                                            saw_non_403_failure = True
                                        last_error = f"Relay error {_relay_status}: {_relay_body}"
                                        # AUTO-HEAL encrypted_content lewat relay
                                        # early-SSE (HTTP 200 + event relay.error):
                                        # buang reasoning replay, mulai dari target 0.
                                    if (
                                        _relay_status == 400
                                        and not healed_once
                                        and _is_encrypted_content_rejection(_relay_body)
                                        and _payload_has_replay_reasoning(payload)
                                    ):
                                        _stripped, removed = _strip_replayed_reasoning(payload)
                                        if removed:
                                            payload = _stripped
                                            healed_once = True
                                            heal_restart = True
                                            relay_target_failed = True
                                            _log(
                                                "RESP",
                                                f"HEAL {target_url} | encrypted_content "
                                                f"ditolak (relay.error) -> buang {removed} "
                                                f"item reasoning replay, retry dari target "
                                                f"pertama",
                                            )
                                            break
                                    relay_target_failed = True
                                    _log(
                                        "RESP",
                                        f"RELAY-ERROR {target_url} | status="
                                        f"{_relay_status} detail={_relay_body!r} "
                                        f"-> target berikutnya",
                                    )
                                    break
                                sent_first_byte = True
                                # Best-effort: intip usage tanpa mengganggu aliran.
                                if '"usage"' in data:
                                    try:
                                        parsed_line = json.loads(data)
                                    except (ValueError, TypeError):
                                        parsed_line = None
                                    found = _extract_responses_usage(parsed_line)
                                    if found:
                                        last_usage = found
                                    else:
                                        # Event response.completed membawa respons
                                        # penuh di .response — intip satu level.
                                        try:
                                            inner = (
                                                parsed_line.get("response")
                                                if isinstance(parsed_line, dict)
                                                else None
                                            )
                                            found = _extract_responses_usage(
                                                {"usage": inner.get("usage")}
                                                if isinstance(inner, dict)
                                                else None
                                            )
                                            if found:
                                                last_usage = found
                                        except (AttributeError, TypeError):
                                            pass
                                data_events += 1
                                yield f"data: {data}\n\n"
                        finally:
                            if pending_line_task is not None:
                                pending_line_task.cancel()
                                with suppress(asyncio.CancelledError, Exception):
                                    await pending_line_task

                    if relay_target_failed:
                        if heal_restart:
                            # Auto-heal: ulangi dari target pertama dengan payload
                            # yang sudah dibersihkan (bukan maju ke target berikut).
                            heal_restart = False
                            target_index = 0
                        else:
                            target_index += 1
                        continue

                    stream_completed = True
                    break

                except (
                    httpx.TimeoutException,
                    httpx.ConnectError,
                    httpx.ReadError,
                    httpx.RemoteProtocolError,
                ) as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                    saw_non_403_failure = True
                    _log("RESP", f"FAIL {target_url} ({type(exc).__name__})")
                    if sent_first_byte:
                        if last_usage:
                            try:
                                _lost_duration = int((time.time() - stream_start) * 1000)
                            except (TypeError, ValueError):
                                _lost_duration = 0
                            background_tasks.add_task(
                                _safe_record,
                                request_id=str(response_id),
                                model=client_model,
                                prompt_tokens=last_usage.get("prompt_tokens", 0),
                                completion_tokens=last_usage.get("completion_tokens", 0),
                                total_tokens=last_usage.get("total_tokens", 0),
                                duration_ms=max(0, _lost_duration),
                            )
                        log_summary("lost")
                        try:
                            yield _sse(
                                {
                                    "error": {
                                        "message": "Stream connection lost",
                                        "detail": str(exc),
                                    }
                                }
                            )
                            yield "data: [DONE]\n\n"
                        except (GeneratorExit, asyncio.CancelledError):
                            raise
                        except Exception:
                            pass
                        return
                    target_index += 1
                    continue

            if stream_completed:
                break
            if (
                not FORBIDDEN_FRESH_SESSION_RETRY
                or fresh_session_retry_done
                or not targets
                or sent_first_byte
                or saw_non_403_failure
                or forbidden_count == 0
                # Identitas WAJIB stabil bila payload membawa replay
                # reasoning encrypted_content (di-issuance ke caller
                # pertama); sesi baru justru menjamin 400 caller-mismatch.
                or _payload_has_replay_reasoning(payload)
            ):
                break
            # Upaya terakhir all-403 (termasuk direct): identitas lama yang
            # di-flag. Rotasi PENUH dengan SATU pasangan (session,
            # prompt_cache_key) baru yang konsisten (lihat
            # _fresh_retry_targets): menguji hipotesis sesi di setiap egress.
            fresh_session_retry_done = True
            old_tag = _oc_session_tag(opencode_headers)
            fresh_targets, fresh_session, _fresh_key = _fresh_retry_targets(
                targets, payload
            )
            if not fresh_targets:
                break
            targets = fresh_targets
            forbidden_count = 0
            saw_non_403_failure = False
            last_error = None
            last_rate_limited = False
            last_retry_after = None
            spurious_429_retried = set()
            _log(
                "RESP",
                f"FRESH-SESSION-RETRY model={client_model} {len(targets)} target "
                f"{old_tag} -> {_oc_session_tag({'x-opencode-session': fresh_session})} "
                f"(rotasi penuh pasangan baru, delay {FORBIDDEN_RETRY_DELAY:.1f}s)",
            )
            await asyncio.sleep(FORBIDDEN_RETRY_DELAY)
            continue

        if not stream_completed:
            log_summary("all-failed")
            if last_rate_limited:
                yield _sse(
                    {
                        "error": {
                            "message": "Upstream rate limited (429)",
                            "detail": last_error or "Rate limited",
                            "code": "RATE_LIMITED",
                            "retry_after": last_retry_after or RATE_LIMIT_BACKOFF,
                        }
                    }
                )
            else:
                yield _sse(
                    {
                        "error": {
                            "message": "All responses targets failed",
                            "detail": last_error or "unknown error",
                        }
                    }
                )
            yield "data: [DONE]\n\n"
            return

        if last_usage:
            try:
                _done_duration = int((time.time() - stream_start) * 1000)
            except (TypeError, ValueError):
                _done_duration = 0
            background_tasks.add_task(
                _safe_record,
                request_id=str(response_id),
                model=client_model,
                prompt_tokens=last_usage.get("prompt_tokens", 0),
                completion_tokens=last_usage.get("completion_tokens", 0),
                total_tokens=last_usage.get("total_tokens", 0),
                duration_ms=max(0, _done_duration),
            )
        log_summary("done")
        yield "data: [DONE]\n\n"

    except asyncio.CancelledError:
        raise
    except GeneratorExit:
        raise
    except Exception as exc:  # noqa: BLE001 - last resort safety net
        try:
            _log("ERROR", f"Responses stream error: {type(exc).__name__}")
            log_summary("error")
            yield _sse({"error": {"message": "Stream error", "code": "STREAM_ERROR"}})
            yield "data: [DONE]\n\n"
        except (GeneratorExit, asyncio.CancelledError):
            raise
        except Exception:
            pass


async def responses_to_chat_stream_generator(
    payload: Dict[str, Any],
    *,
    client_model: str,
    include_usage_requested: bool,
    background_tasks: BackgroundTasks,
    use_relay: bool,
    opencode_headers: Optional[Dict[str, str]] = None,
):
    """Jembatani SSE Responses upstream menjadi SSE chat untuk klien chat-only.

    Dipakai HANYA saat model Responses-only (muse-spark) diminta lewat
    /v1/chat/completions. Request langsung ke /v1/responses TIDAK lewat sini
    (ditangani responses_stream_generator yang pass-through mentah).
    Event Responses (`response.output_text.delta`,
    `response.function_call_arguments.delta`, `response.completed`, ...)
    diterjemahkan menjadi delta chat (`content` / `tool_calls`).
    Event reasoning (`response.reasoning_*.delta`) diteruskan sebagai
    `reasoning_content` agar socket klien tetap menerima `data:` selama fase
    thinking panjang (tanpa ini klien idle-timeout ~90s walau wire aktif).
    Rotasi relay, 429-cooldown, idle timeout, dan keepalive sama dengan
    responses_stream_generator.
    """
    stream_id = f"chatcmpl-{secrets.token_hex(16)}"
    created = int(time.time())
    stream_start = time.time()
    sent_role = False
    sent_payload = False
    stream_completed = False
    saw_tool_call = False
    saw_text_content = False
    wire_bytes = 0
    data_events = 0
    event_type_counts: Dict[str, int] = {}
    item_kinds: Dict[str, int] = {}
    reasoning_events = 0
    reasoning_chars = 0
    reasoning_buffer: List[str] = []
    reasoning_buffer_chars = 0
    first_reasoning_at: Optional[float] = None
    last_reasoning_at: Optional[float] = None
    first_content_at: Optional[float] = None
    raw_preview: List[str] = []  # baris non-SSE pertama, untuk diagnosis empty-stream
    last_usage: Optional[Dict[str, int]] = None
    call_index_by_key: Dict[str, int] = {}
    call_ids: List[str] = []
    call_id_emitted: set = set()

    def chunk(delta: Dict[str, Any], finish_reason: Optional[str] = None) -> str:
        return _sse(
            {
                "id": stream_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": client_model,
                "choices": [
                    {"index": 0, "delta": delta, "finish_reason": finish_reason},
                ],
            }
        )

    def role_chunk() -> str:
        nonlocal sent_role
        if sent_role:
            return ""
        sent_role = True
        return chunk({"role": "assistant"})

    def log_summary(note: str = "") -> None:
        try:
            if first_reasoning_at is not None and last_reasoning_at is not None:
                reasoning_dur = f"{last_reasoning_at - first_reasoning_at:.2f}s"
            else:
                reasoning_dur = "-"
            _log(
                "RESP",
                f"BRIDGE-SUMMARY id={stream_id[:8]} {note} model={client_model} "
                f"wire_bytes={wire_bytes} events={data_events} "
                f"types={_format_event_types(event_type_counts)} "
                f"items={_format_event_types(item_kinds)} "
                f"reasoning:events={reasoning_events} chars={reasoning_chars} dur={reasoning_dur} "
                f"content_start={'+%.2fs' % (first_content_at - stream_start) if first_content_at is not None else '-'} "
                f"total={time.time() - stream_start:.2f}s",
            )
        except Exception:  # noqa: BLE001 - diagnostik tak boleh crash
            pass

    def record_final_usage() -> None:
        if not last_usage:
            return
        try:
            prompt_tokens = int(last_usage.get("prompt_tokens", 0) or 0)
            completion_tokens = int(last_usage.get("completion_tokens", 0) or 0)
            total_tokens = int(last_usage.get("total_tokens", 0) or 0)
        except (TypeError, ValueError):
            return
        try:
            _bridge_duration = int((time.time() - stream_start) * 1000)
        except (TypeError, ValueError):
            _bridge_duration = 0
        background_tasks.add_task(
            _safe_record,
            request_id=stream_id,
            model=client_model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            duration_ms=max(0, _bridge_duration),
        )

    base_headers = _stream_request_headers(opencode_headers)
    targets: List[Tuple[str, Dict[str, str]]] = []
    if use_relay:
        for relay_url in _relay_batch_for_request(for_stream=True):
            targets.append((relay_url, _with_relay_headers(OPENCODE_RESPONSES_URL, base_headers)))
        if RELAY_FALLBACK:
            targets.append((OPENCODE_RESPONSES_URL, dict(base_headers)))
    else:
        targets.append((OPENCODE_RESPONSES_URL, dict(base_headers)))
    targets = _limit_stream_targets(targets)
    vision_direct_first = use_relay and RELAY_FALLBACK and _payload_has_media(payload)
    if vision_direct_first:
        # Vision: direct DULU; relay HANYA fallback bila direct 429
        # (lihat guard di loop). Biner base64 rawan 413/504 relay.
        direct = [t for t in targets if "x-relay-target" not in t[1]]
        relays = [t for t in targets if "x-relay-target" in t[1]]
        targets = direct + relays
        _log("RESP", "VISION: direct dulu, relay khusus 429")
    if DIRECT_FIRST_SLOW and (
        _is_responses_only_model(client_model) or _is_giant_payload(payload)
    ):
        # Generator ini KHUSUS model Responses-only (spark & co.): thinking
        # xhigh rutin >25s sehingga relay selalu 504 tanpa byte. Direct dulu
        # agar thinking jalan sekali + klien seperti Hermes yang mengabaikan
        # keepalive `:` tidak reconnect dalam loop stall.
        targets = _order_targets_direct_first(targets)
        _log("RESP", f"SLOW model={client_model}: direct dulu, relay fallback")

    last_error: Optional[str] = None
    last_rate_limited = False
    last_retry_after: Optional[float] = None
    # Bug spam-429 opencode khusus muse-spark: 1x same-route retry per target.
    spurious_429_retried: set = set()
    # AUTO-HEAL encrypted_content: sekali per request, kembali ke target 0.
    healed_once = False
    heal_restart = False
    # Pelacakan upaya terakhir fresh-session (sama seperti generator lain):
    # hanya bila SETIAP kegagalan adalah 403 pra-payload dan TIDAK ADA
    # kegagalan lain. Dilewati bila payload membawa replay reasoning
    # (identitas wajib stabil) — lihat blok fase di bawah.
    forbidden_count = 0
    saw_non_403_failure = False
    fresh_session_retry_done = False
    _log("RESP", f"CHAT-BRIDGE-STREAM model={client_model} keys={sorted(payload.keys())} {_oc_session_tag(opencode_headers)}")


    try:
        # Fase luar (while True) hanya berputar SEKALI ekstra: fase
        # fresh-session sebagai upaya terakhir all-403 (lihat bawah).
        while True:
            target_index = 0
            while target_index < len(targets):
                target_url, headers = targets[target_index]
                is_relay = "x-relay-target" in headers
                if vision_direct_first and is_relay and not last_rate_limited:
                    # Relay vision hanya untuk 429 direct; kegagalan lain
                    # selesai di direct (last_error sudah terisi).
                    break
                # Request ID fresh per attempt ala CLI asli (msg_ unik per POST).
                headers = _fresh_request_headers(headers)
                _log(
                    "RESP",
                    f"ATTEMPT {target_index + 1}/{len(targets)} "
                    f"{'RELAY' if is_relay else 'DIRECT'} target={target_url}",
                )
                try:
                    termination_seen = False
                    relay_target_failed = False
                    client = _get_http()
                    # Konteks panjang butuh TTFB lama: read-timeout per-request
                    # BRIDGE_REQUEST_TIMEOUT (bukan global 120s). Heartbeat relay
                    # 10 detik menjaga wire tetap aktif, jadi read-timeout hanya
                    # menembak bila stream benar-benar macet.
                    _bridge_timeout = httpx.Timeout(
                        connect=10.0,
                        read=BRIDGE_REQUEST_TIMEOUT,
                        write=10.0,
                        pool=10.0,
                    )
                    async with client.stream(
                        "POST", target_url, json=payload, headers=headers,
                        timeout=_bridge_timeout,
                    ) as response:
                        if response.status_code != 200:
                            try:
                                body = await response.aread()
                                detail = body.decode("utf-8", errors="replace")[:500]
                            except Exception:  # noqa: BLE001
                                detail = ""
                            if is_relay and _is_relay_timeout(response):
                                # Konteks raksasa -> 504 Edge wajar (TTFB>25s),
                                # jangan tandai relay sehat sebagai broken.
                                if _should_mark_stream_broken(target_url, payload):
                                    _mark_relay_stream_broken(
                                        target_url,
                                        time.time() + RELAY_STREAM_BROKEN_COOLDOWN,
                                    )
                                last_error = f"Relay stream timeout (504): {detail[:200]}"
                                saw_non_403_failure = True
                                target_index += 1
                                _log(
                                    "RESP",
                                    f"STREAM-TIMEOUT {target_url} | Vercel kill 504 "
                                    f"(limit eksekusi ~25s, bukan bug proxy) -> relay "
                                    f"di-skip streaming {RELAY_STREAM_BROKEN_COOLDOWN:.0f}s, "
                                    f"lanjut ke target berikutnya/direct",
                                )
                                continue
                            if response.status_code == 429 and not sent_payload:
                                # PENGECUALIAN bug spam-429 muse-spark: retry 1x
                                # same-route dulu sebelum rotasi/cooldown.
                                if _should_retry_same_route_429(client_model) and target_url not in spurious_429_retried:
                                    spurious_429_retried.add(target_url)
                                    _delay = _retry_after_seconds(response, RATE_LIMIT_BACKOFF)
                                    _log(
                                        "RESP",
                                        f"SPURIOUS-429 {target_url} | retry same-route 1x "
                                        f"in {_delay:.1f}s sebelum ganti route",
                                    )
                                    await asyncio.sleep(_delay)
                                    continue  # ulangi target_index yang sama
                                rate_cls = _classify_rate_limit(response)
                                last_retry_after = _retry_after_seconds(
                                    response, RATE_LIMIT_BACKOFF
                                )
                                _mark_relay_rate_limited(
                                    target_url,
                                    time.time() + _relay_cooldown_seconds(response),
                                )
                                last_error = f"Rate limited (429): {detail}"
                                last_rate_limited = True
                                saw_non_403_failure = True
                                _log(
                                    "RESP",
                                    f"RATE-LIMITED {target_url} | "
                                    f"{rate_cls['description']} | detail={detail[:300]!r}",
                                )
                                target_index += 1
                                continue
                            last_error = (
                                f"Upstream responded with {response.status_code}: {detail}"
                            )
                            last_rate_limited = response.status_code == 429
                            if response.status_code == 403:
                                # Dihitung relay+direct (kandidat fresh-session retry).
                                forbidden_count += 1
                            else:
                                saw_non_403_failure = True
                            if response.status_code == 403 and is_relay and not sent_payload:
                                _mark_relay_forbidden(
                                    target_url,
                                    time.time() + RELAY_403_COOLDOWN,
                                )
                                _log(
                                    "RESP",
                                    f"FORBIDDEN {target_url} | upstream 403 "
                                    f"(IP relay di-flag, bukan salah fingerprint) "
                                    f"-> relay di-cooldown {RELAY_403_COOLDOWN:.0f}s",
                                )
                            target_index += 1
                            _log(
                                "RESP",
                                f"FAIL {target_url} | upstream-status={response.status_code} "
                                f"detail={detail[:300]!r}",
                            )
                            continue

                        _log("RESP", f"OK {target_url} (bridge)")
                        content_type = (response.headers.get("content-type") or "").lower()
                        if "text/event-stream" not in content_type:
                            # Relay/CDN men-buffer SSE menjadi SATU body JSON utuh
                            # (content-type application/json). Jangan dilewatkan ke
                            # loop baris — konversi langsung menjadi chunk chat.
                            try:
                                raw_body = await response.aread()
                                # Catat byte agar SUMMARY tidak 0 (buffered JSON
                                # bukan SSE baris-per-baris, tapi tetap sukses).
                                wire_bytes += len(raw_body or b"")
                                data_events += 1
                                buffered = json.loads(raw_body.decode("utf-8"))
                            except (ValueError, TypeError, UnicodeDecodeError):
                                buffered = None
                            if isinstance(buffered, dict) and (
                                buffered.get("object") == "response"
                                or "output" in buffered
                            ):
                                content_b, calls_b = _responses_output_to_chat(
                                    buffered.get("output")
                                )
                                found_b = _extract_responses_usage(buffered)
                                if found_b:
                                    last_usage = found_b
                                if content_b or calls_b:
                                    role = role_chunk()
                                    if role:
                                        sent_payload = True
                                        yield role
                                    sent_payload = True
                                    if first_content_at is None:
                                        first_content_at = time.time()
                                    if content_b:
                                        saw_text_content = True
                                        yield chunk({"content": content_b})
                                    for idx_b, call_b in enumerate(calls_b):
                                        call_b = dict(call_b)
                                        call_b["index"] = idx_b
                                        saw_tool_call = True
                                        yield chunk({"tool_calls": [call_b]})
                                else:
                                    reasoning_b = _extract_responses_reasoning_text(
                                        buffered.get("output")
                                    )
                                    if reasoning_b and REASONING_FORWARD:
                                        now_b = time.time()
                                        reasoning_events += 1
                                        reasoning_chars += len(reasoning_b)
                                        reasoning_buffer.append(reasoning_b[:20000])
                                        reasoning_buffer_chars += min(len(reasoning_b), 20000)
                                        if first_reasoning_at is None:
                                            first_reasoning_at = now_b
                                        last_reasoning_at = now_b
                                        role = role_chunk()
                                        if role:
                                            sent_payload = True
                                            yield role
                                        sent_payload = True
                                        yield chunk(
                                            {"reasoning_content": reasoning_b[:20000]}
                                        )
                                termination_seen = True
                                stream_completed = True
                                break
                            last_error = (
                                "Upstream returned "
                                f"{content_type or 'unknown content-type'} instead of SSE: "
                                f"{(raw_body[:200] if 'raw_body' in dir() else b'').decode('utf-8', errors='replace')}"
                            )
                            target_index += 1
                            _log("RESP", f"FAIL {target_url} | {last_error[:300]}")
                            continue
                        line_iter = response.aiter_lines()
                        pending_line_task: Optional[asyncio.Task[str]] = None
                        last_activity_at = time.time()
                        try:
                            while True:
                                idle_seconds = time.time() - last_activity_at
                                # Bridge memakai BRIDGE_REQUEST_TIMEOUT (konteks
                                # panjang = TTFB wajar 120-300s), bukan global 120s.
                                if idle_seconds > BRIDGE_REQUEST_TIMEOUT:
                                    raise asyncio.TimeoutError(
                                        f"No upstream progress for {int(idle_seconds)}s"
                                        f" (idle timeout {BRIDGE_REQUEST_TIMEOUT:.0f}s)"
                                    )
                                if pending_line_task is None:
                                    pending_line_task = asyncio.create_task(
                                        line_iter.__anext__()
                                    )
                                done, _ = await asyncio.wait(
                                    (pending_line_task,),
                                    timeout=min(
                                        SSE_KEEPALIVE_INTERVAL,
                                        max(0.0, BRIDGE_REQUEST_TIMEOUT - idle_seconds),
                                    ),
                                    return_when=asyncio.FIRST_COMPLETED,
                                )
                                if not done:
                                    yield ":\n\n"
                                    continue
                                try:
                                    line = pending_line_task.result()
                                except StopAsyncIteration:
                                    stream_completed = True
                                    break
                                finally:
                                    pending_line_task = None

                                if not line:
                                    continue
                                if line.startswith(":"):
                                    continue
                                if line.startswith("data:"):
                                    data = line[5:].lstrip()
                                else:
                                    # Toleransi: sebagian relay/middlebox mengirim
                                    # event Responses sebagai baris JSON telanjang
                                    # (tanpa prefix "data:"). Terima bila bentuknya
                                    # event Responses, abaikan selain itu.
                                    try:
                                        probe = json.loads(line)
                                    except (ValueError, TypeError):
                                        continue
                                    if not isinstance(probe, dict) or not str(
                                        probe.get("type", "")
                                    ).startswith("response."):
                                        if len(raw_preview) < 5:
                                            raw_preview.append(line[:200])
                                        continue
                                    data = line
                                wire_bytes += len(data)
                                last_activity_at = time.time()
                                if data == "[DONE]":
                                    termination_seen = True
                                    stream_completed = True
                                    break
                                try:
                                    event = json.loads(data)
                                except (ValueError, TypeError):
                                    continue
                                if not isinstance(event, dict):
                                    continue
                                # Relay early-SSE: fetch upstream gagal dilaporkan
                                # sebagai event (HTTP sudah 200). Cegat SEBELUM
                                # payload agar failover ke target berikutnya jalan.
                                if event.get("type") == "relay.error" and not sent_payload:
                                    _relay_status = event.get("status")
                                    _relay_body = str(event.get("body") or "")[:300]
                                    if _relay_status == 429:
                                        last_retry_after = RATE_LIMIT_BACKOFF
                                        last_rate_limited = True
                                        saw_non_403_failure = True
                                        _mark_relay_rate_limited(
                                            target_url,
                                            time.time() + RATE_LIMIT_COOLDOWN,
                                        )
                                    elif _relay_status == 403:
                                        last_rate_limited = False
                                        forbidden_count += 1
                                        _mark_relay_forbidden(
                                            target_url,
                                            time.time() + RELAY_403_COOLDOWN,
                                        )
                                    else:
                                        last_rate_limited = _relay_status == 429
                                        saw_non_403_failure = True
                                    last_error = f"Relay error {_relay_status}: {_relay_body}"
                                    # AUTO-HEAL encrypted_content lewat relay
                                    # early-SSE (HTTP 200 + event relay.error).
                                    if (
                                        _relay_status == 400
                                        and not healed_once
                                        and _is_encrypted_content_rejection(_relay_body)
                                        and _payload_has_replay_reasoning(payload)
                                    ):
                                        _stripped, removed = _strip_replayed_reasoning(payload)
                                        if removed:
                                            payload = _stripped
                                            healed_once = True
                                            heal_restart = True
                                            relay_target_failed = True
                                            _log(
                                                "RESP",
                                                f"HEAL {target_url} | encrypted_content "
                                                f"ditolak (relay.error bridge) -> buang "
                                                f"{removed} item reasoning replay, retry "
                                                f"dari target pertama",
                                            )
                                            break
                                    relay_target_failed = True
                                    _log(
                                        "RESP",
                                        f"RELAY-ERROR {target_url} | status="
                                        f"{_relay_status} detail={_relay_body!r} "
                                        f"-> target berikutnya",
                                    )
                                    break
                                # Relay early-SSE membungkus respons buffered
                                # (satu JSON utuh) sebagai satu event SSE —
                                # konversi langsung seperti jalur buffered.
                                if event.get("object") == "response" or (
                                    isinstance(event.get("output"), list)
                                    and not str(event.get("type", "")).startswith("response.")
                                ):
                                    content_f, calls_f = _responses_output_to_chat(
                                        event.get("output")
                                    )
                                    found_f = _extract_responses_usage(event)
                                    if found_f:
                                        last_usage = found_f
                                    if content_f or calls_f:
                                        role = role_chunk()
                                        if role:
                                            sent_payload = True
                                            yield role
                                        sent_payload = True
                                        if first_content_at is None:
                                            first_content_at = time.time()
                                        if content_f:
                                            saw_text_content = True
                                            yield chunk({"content": content_f})
                                        for idx_f, call_f in enumerate(calls_f):
                                            call_f = dict(call_f)
                                            call_f["index"] = idx_f
                                            saw_tool_call = True
                                            yield chunk({"tool_calls": [call_f]})
                                    else:
                                        # Full-object tanpa message/tool (mis. hanya
                                        # reasoning): teruskan reasoning agar klien
                                        # tidak menerima stream kosong.
                                        reasoning_f = _extract_responses_reasoning_text(
                                            event.get("output")
                                        )
                                        if reasoning_f and REASONING_FORWARD:
                                            now_f = time.time()
                                            reasoning_events += 1
                                            reasoning_chars += len(reasoning_f)
                                            # Hormati batas 20rb char seperti jalur delta
                                            # (satu full-object bisa 20rb char per event).
                                            if reasoning_buffer_chars < 20000:
                                                reasoning_buffer.append(reasoning_f[:20000 - reasoning_buffer_chars])
                                                reasoning_buffer_chars += min(len(reasoning_f), 20000 - reasoning_buffer_chars)
                                            if first_reasoning_at is None:
                                                first_reasoning_at = now_f
                                                _log(
                                                    "RESP",
                                                    f"REASONING start +{now_f - stream_start:.2f}s (buffered)",
                                                )
                                            last_reasoning_at = now_f
                                            role = role_chunk()
                                            if role:
                                                sent_payload = True
                                                yield role
                                            sent_payload = True
                                            yield chunk(
                                                {"reasoning_content": reasoning_f[:20000]}
                                            )
                                    termination_seen = True
                                    stream_completed = True
                                    break
                                data_events += 1
                                event_type = event.get("type", "")
                                if isinstance(event_type, str) and event_type:
                                    if len(event_type_counts) < 32 or event_type in event_type_counts:
                                        event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1

                                # Reasoning upstream: teruskan sebagai
                                # `reasoning_content` agar klien menerima `data:`
                                # selama fase thinking (mencegah idle-timeout
                                # ~90s di Hermes/SDK yang mengabaikan `:`
                                # keepalive). Pola sama dengan REASONING_FORWARD
                                # di stream_generator chat.
                                reasoning_frag = _extract_responses_reasoning_delta(event)
                                if reasoning_frag:
                                    now = time.time()
                                    reasoning_events += 1
                                    reasoning_chars += len(reasoning_frag)
                                    # Simpan untuk fallback anti-kosong (dibatasi
                                    # agar memori datar bila reasoning sangat panjang).
                                    if reasoning_buffer_chars < 20000:
                                        reasoning_buffer.append(reasoning_frag)
                                        reasoning_buffer_chars += len(reasoning_frag)
                                    if first_reasoning_at is None:
                                        first_reasoning_at = now
                                        _log(
                                            "RESP",
                                            f"REASONING start +{now - stream_start:.2f}s",
                                        )
                                    last_reasoning_at = now
                                    if REASONING_FORWARD:
                                        role = role_chunk()
                                        if role:
                                            sent_payload = True
                                            yield role
                                        sent_payload = True
                                        yield chunk(
                                            {"reasoning_content": reasoning_frag}
                                        )
                                    continue

                                if event_type in (
                                    "response.output_text.delta",
                                    "response.text.delta",
                                ):
                                    text = event.get("delta", "")
                                    if not isinstance(text, str):
                                        text = str(text) if text is not None else ""
                                    if text:
                                        role = role_chunk()
                                        if role:
                                            sent_payload = True
                                            yield role
                                        sent_payload = True
                                        saw_text_content = True
                                        if first_content_at is None:
                                            first_content_at = time.time()
                                        yield chunk({"content": text})
                                elif event_type == "response.output_item.added":
                                    item = event.get("item") or {}
                                    _count_item_kind(item_kinds, "added", item)
                                    if isinstance(item, dict) and item.get("type") == "reasoning":
                                        # Awal blok reasoning (deltas menyusul di
                                        # event reasoning_*.delta di atas).
                                        if first_reasoning_at is None:
                                            first_reasoning_at = time.time()
                                        continue
                                    if isinstance(item, dict) and item.get("type") == "function_call":
                                        key = str(
                                            event.get("output_index", item.get("item_id", len(call_ids)))
                                        )
                                        if key not in call_index_by_key:
                                            call_index_by_key[key] = len(call_ids)
                                            call_ids.append(
                                                item.get("call_id") or item.get("item_id") or f"call_{len(call_ids)}"
                                            )
                                        idx = call_index_by_key[key]
                                        call_id = call_ids[idx]
                                        saw_tool_call = True
                                        role = role_chunk()
                                        if role:
                                            sent_payload = True
                                            yield role
                                        sent_payload = True
                                        call_id_emitted.add(call_id)
                                        yield chunk(
                                            {
                                                "tool_calls": [
                                                    {
                                                        "index": idx,
                                                        "id": call_id,
                                                        "type": "function",
                                                        "function": {
                                                            "name": item.get("name") or "",
                                                            "arguments": "",
                                                        },
                                                    }
                                                ]
                                            }
                                        )
                                elif event_type == "response.output_item.done":
                                    # Item lengkap (thinking/message/tool) yang
                                    # dikirim utuh di akhir TANPA delta
                                    # sebelumnya — mis. reasoning xhigh yang
                                    # tidak men-streaming summary. Tanpa cabang
                                    # ini, thinking puluhan KB tidak terlihat
                                    # klien (reasoning_events=0) dan stream
                                    # berakhir EMPTY. Guard anti-duplikat:
                                    # teruskan hanya yang belum mengalir.
                                    done_item = event.get("item") or {}
                                    if not isinstance(done_item, dict):
                                        continue
                                    done_kind = done_item.get("type")
                                    _count_item_kind(item_kinds, "done", done_item)
                                    if done_kind == "reasoning":
                                        done_text = _extract_responses_reasoning_text(
                                            [done_item]
                                        )
                                        if done_text:
                                            now = time.time()
                                            if reasoning_buffer_chars < 20000:
                                                reasoning_buffer.append(
                                                    done_text[:20000 - reasoning_buffer_chars]
                                                )
                                                reasoning_buffer_chars += min(
                                                    len(done_text), 20000 - reasoning_buffer_chars
                                                )
                                            # Forward live hanya bila belum ada
                                            # reasoning delta (hindari thinking
                                            # ganda di layar klien).
                                            if reasoning_events == 0:
                                                if first_reasoning_at is None:
                                                    first_reasoning_at = now
                                                    _log(
                                                        "RESP",
                                                        f"REASONING start +{now - stream_start:.2f}s (item.done)",
                                                    )
                                                last_reasoning_at = now
                                                if REASONING_FORWARD:
                                                    role = role_chunk()
                                                    if role:
                                                        sent_payload = True
                                                        yield role
                                                    sent_payload = True
                                                    reasoning_events += 1
                                                    reasoning_chars += len(done_text)
                                                    yield chunk(
                                                        {"reasoning_content": done_text[:20000]}
                                                    )
                                    elif done_kind == "message":
                                        if not saw_text_content:
                                            content_d, _calls_d = _responses_output_to_chat(
                                                [done_item]
                                            )
                                            if content_d:
                                                role = role_chunk()
                                                if role:
                                                    sent_payload = True
                                                    yield role
                                                sent_payload = True
                                                saw_text_content = True
                                                if first_content_at is None:
                                                    first_content_at = time.time()
                                                yield chunk({"content": content_d})
                                    elif done_kind == "function_call":
                                        key = str(
                                            event.get("output_index", done_item.get("item_id", len(call_ids)))
                                        )
                                        if key not in call_index_by_key:
                                            call_index_by_key[key] = len(call_ids)
                                            call_ids.append(
                                                done_item.get("call_id") or done_item.get("id") or f"call_{len(call_ids)}"
                                            )
                                        idx = call_index_by_key[key]
                                        call_id = call_ids[idx]
                                        if call_id not in call_id_emitted:
                                            args = done_item.get("arguments", "{}")
                                            if isinstance(args, (dict, list)):
                                                args = json.dumps(args, ensure_ascii=False)
                                            saw_tool_call = True
                                            role = role_chunk()
                                            if role:
                                                sent_payload = True
                                                yield role
                                            sent_payload = True
                                            call_id_emitted.add(call_id)
                                            yield chunk(
                                                {
                                                    "tool_calls": [
                                                        {
                                                            "index": idx,
                                                            "id": call_id,
                                                            "type": "function",
                                                            "function": {
                                                                "name": done_item.get("name") or "",
                                                                "arguments": str(args if args is not None else "{}"),
                                                            },
                                                        }
                                                    ]
                                                }
                                            )
                                elif event_type == "response.function_call_arguments.delta":
                                    key = str(event.get("output_index", event.get("item_id", "")))
                                    if key not in call_index_by_key:
                                        call_index_by_key[key] = len(call_ids)
                                        call_ids.append(
                                            event.get("item_id") or f"call_{len(call_ids)}"
                                        )
                                    idx = call_index_by_key[key]
                                    call_id = call_ids[idx]
                                    fragment = event.get("delta", "")
                                    if not isinstance(fragment, str):
                                        fragment = str(fragment) if fragment is not None else ""
                                    if fragment:
                                        saw_tool_call = True
                                        role = role_chunk()
                                        if role:
                                            sent_payload = True
                                            yield role
                                        sent_payload = True
                                        entry: Dict[str, Any] = {
                                            "index": idx,
                                            "type": "function",
                                            "function": {"arguments": fragment},
                                        }
                                        if call_id not in call_id_emitted:
                                            entry["id"] = call_id
                                            call_id_emitted.add(call_id)
                                        yield chunk({"tool_calls": [entry]})
                                elif event_type == "response.created":
                                    role = role_chunk()
                                    if role:
                                        yield role
                                elif event_type in (
                                    "response.completed",
                                    "response.failed",
                                    "response.incomplete",
                                ):
                                    inner = event.get("response") or {}
                                    found = _extract_responses_usage({"usage": inner.get("usage")})
                                    if found:
                                        last_usage = found
                                    termination_seen = True
                                    stream_completed = True
                                    break
                        finally:
                            if pending_line_task is not None:
                                pending_line_task.cancel()
                                with suppress(asyncio.CancelledError, Exception):
                                    await pending_line_task

                    if relay_target_failed:
                        if heal_restart:
                            # Auto-heal: ulangi dari target pertama dengan payload
                            # yang sudah dibersihkan (bukan maju ke target berikut).
                            heal_restart = False
                            target_index = 0
                        else:
                            target_index += 1
                        continue

                    stream_completed = True
                    break

                except (
                    httpx.TimeoutException,
                    httpx.ConnectError,
                    httpx.ReadError,
                    httpx.RemoteProtocolError,
                ) as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                    saw_non_403_failure = True
                    _log("RESP", f"FAIL {target_url} ({type(exc).__name__})")
                    if sent_payload:
                        record_final_usage()
                        log_summary("lost")
                        try:
                            yield _sse(
                                {
                                    "error": {
                                        "message": "Stream connection lost",
                                        "detail": str(exc),
                                    }
                                }
                            )
                            yield "data: [DONE]\n\n"
                        except (GeneratorExit, asyncio.CancelledError):
                            raise
                        except Exception:
                            pass
                        return
                    target_index += 1
                    continue

            if stream_completed:
                break
            if (
                not FORBIDDEN_FRESH_SESSION_RETRY
                or fresh_session_retry_done
                or not targets
                or sent_payload
                or saw_non_403_failure
                or forbidden_count == 0
                # Identitas WAJIB stabil bila payload membawa replay
                # reasoning encrypted_content (lihat generator-1).
                or _payload_has_replay_reasoning(payload)
            ):
                break
            # Upaya terakhir all-403 (termasuk direct): rotasi PENUH
            # dengan SATU pasangan (session, prompt_cache_key) baru yang
            # konsisten (lihat _fresh_retry_targets).
            fresh_session_retry_done = True
            old_tag = _oc_session_tag(opencode_headers)
            fresh_targets, fresh_session, _fresh_key = _fresh_retry_targets(
                targets, payload
            )
            if not fresh_targets:
                break
            targets = fresh_targets
            forbidden_count = 0
            saw_non_403_failure = False
            last_error = None
            last_rate_limited = False
            last_retry_after = None
            spurious_429_retried = set()
            _log(
                "RESP",
                f"FRESH-SESSION-RETRY model={client_model} {len(targets)} target "
                f"{old_tag} -> {_oc_session_tag({'x-opencode-session': fresh_session})} "
                f"(rotasi penuh pasangan baru, delay {FORBIDDEN_RETRY_DELAY:.1f}s)",
            )
            await asyncio.sleep(FORBIDDEN_RETRY_DELAY)
            continue

        if not stream_completed:
            log_summary("all-failed")
            if last_rate_limited:
                yield _sse(
                    {
                        "error": {
                            "message": "Upstream rate limited (429)",
                            "detail": last_error or "Rate limited",
                            "code": "RATE_LIMITED",
                            "retry_after": last_retry_after or RATE_LIMIT_BACKOFF,
                        }
                    }
                )
            else:
                yield _sse(
                    {
                        "error": {
                            "message": "All responses targets failed",
                            "detail": last_error or "unknown error",
                        }
                    }
                )
            yield "data: [DONE]\n\n"
            return

        if not sent_payload:
            # Stream 200-OK tapi NOL konten (kasus muse-spark: wire_bytes=0,
            # events=0). Finish kosong membuat Hermes "empty content after
            # retries" — kirim error eksplisit agar bisa di-retry/fallback.
            log_summary("empty")
            _log(
                "RESP",
                f"EMPTY-STREAM model={client_model} wire_bytes={wire_bytes} "
                f"events={data_events} reasoning_events={reasoning_events} "
                f"types={_format_event_types(event_type_counts)} "
                f"items={_format_event_types(item_kinds)} "
                f"budget={payload.get('max_output_tokens') if isinstance(payload, dict) else '?'} "
                f"preview={raw_preview[:5]!r}",
            )
            yield _sse(
                {
                    "error": {
                        "message": "Upstream returned an empty response",
                        "detail": last_error or "no content or tool calls received",
                        "code": "EMPTY_RESPONSE",
                    }
                }
            )
            yield "data: [DONE]\n\n"
            return

        # Fallback anti-kosong: stream punya reasoning tapi tanpa content/tool
        # (mis. max_output_tokens habis untuk thinking). Klien chat-only yang
        # mengabaikan `reasoning_content` tetap butuh `content` agar tidak
        # dianggap kosong — teruskan reasoning sebagai isi. Pola sama dengan
        # stream_generator chat.
        if not saw_text_content and not saw_tool_call and reasoning_buffer:
            reasoning_visible = "".join(reasoning_buffer)[:20000].strip()
            if reasoning_visible:
                role = role_chunk()
                if role:
                    yield role
                saw_text_content = True
                if first_content_at is None:
                    first_content_at = time.time()
                yield chunk({"content": reasoning_visible})

        if not sent_role:
            yield role_chunk()
        record_final_usage()
        finish_reason = "tool_calls" if saw_tool_call else "stop"
        yield chunk({}, finish_reason)
        if last_usage and include_usage_requested:
            yield _sse(
                {
                    "id": stream_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": client_model,
                    "choices": [],
                    "usage": last_usage,
                }
            )
        log_summary(f"done finish={finish_reason}")
        yield "data: [DONE]\n\n"

    except asyncio.CancelledError:
        raise
    except GeneratorExit:
        raise
    except Exception as exc:  # noqa: BLE001 - last resort safety net
        try:
            _log("ERROR", f"Bridge stream error: {type(exc).__name__}")
            log_summary("error")
            yield _sse({"error": {"message": "Stream error", "code": "STREAM_ERROR"}})
            yield "data: [DONE]\n\n"
        except (GeneratorExit, asyncio.CancelledError):
            raise
        except Exception:
            pass

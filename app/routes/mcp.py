"""Route group: mcp (Model Context Protocol server).

Mengekspos gateway sebagai MCP server via Streamable HTTP (spec 2025-03-26 /
2025-06-18, kompatibel mundur 2024-11-05) + SSE legacy best-effort:

- `POST /mcp` — JSON-RPC 2.0 utama (initialize, ping, tools/list, tools/call,
  prompts/list, resources/list, ...). Single object maupun batch array.
  Notifikasi (tanpa `id`) -> 202 tanpa body.
- `GET /mcp` — discovery JSON ramah curl/browser (bukan SSE hanging).
- `DELETE /mcp` — terminasi sesi (stateless: no-op 200).
- `GET /sse` + `POST /messages` — transport SSE legacy (2024-11-05)
  best-effort untuk klien lama (Claude Desktop lama / opencode lama).

Tools yang diekspos (lihat `_MCP_TOOLS`):
- `chat` — inferensi chat OpenAI-compatible untuk SEMUA model, termasuk
  muse-spark (Responses-only) via bridge otomatis `chat -> Responses -> chat`.
- `responses` — Responses API untuk SEMUA model (native pass-through untuk
  muse-spark/gpt/grok; reverse-bridge via chat pipeline untuk model lain).
- `list_models` — daftar model free live (tanpa inferensi).
- `gateway_props` — snapshot konfigurasi read-only (tanpa upstream call).

Auth mengikuti gateway (`verify_gateway_key`): bila `GATEWAY_API_KEYS` diisi,
MCP clients wajib kirim `Authorization: Bearer <key>` / `x-api-key: <key>`
(sebagai `headers` di opencode.json `mcp` remote) — sama seperti `/v1/*`.
Mode stateless: tiap request independen, `mcp-session-id` klien di-echo bila
dikirim tapi tidak diwajibkan.
"""
from __future__ import annotations

import json
import secrets
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from app.security.gateway_auth import verify_gateway_key

router = APIRouter()

MCP_PROTOCOL_VERSIONS = ("2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25")
MCP_LATEST = "2025-06-18"


def _server_info() -> Dict[str, str]:
    try:
        from app.core.config import APP_VERSION
        version = str(APP_VERSION)
    except (ImportError, AttributeError, TypeError, ValueError):
        version = "2.0.0"
    return {"name": "sinug-gateway", "version": version}


_MCP_TOOLS: List[Dict[str, Any]] = [
    {
        "name": "chat",
        "description": (
            "Chat completions via Sinug gateway (OpenAI-compatible). Works for ALL "
            "free models including muse-spark (Responses-only, auto-bridged). "
            "Use for general chat, agentic tool loops (tool_calls returned), and "
            "MCP-style function calling."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "model": {"type": "string", "description": "Model id from list_models (e.g. muse-spark-1.3-contributor-free)"},
                "messages": {
                    "type": "array",
                    "description": "OpenAI chat messages [{role, content, ...}]. Required unless `prompt` is given.",
                    "items": {"type": "object"},
                },
                "prompt": {"type": "string", "description": "Shorthand for a single user message (alternative to `messages`)."},
                "system": {"type": "string", "description": "Optional system prompt prepended to messages."},
                "temperature": {"type": "number", "description": "Sampling temperature (default 0.7)."},
                "max_tokens": {"type": "integer", "description": "Max output tokens (default 65536)."},
                "tools": {
                    "type": "array",
                    "description": "Optional function tools (OpenAI shape or MCP shape with inputSchema — normalized automatically).",
                    "items": {"type": "object"},
                },
                "tool_choice": {"description": "Upstream only supports 'auto'; other values are coerced to 'auto' when tools exist."},
            },
            "required": ["model"],
        },
    },
    {
        "name": "responses",
        "description": (
            "Responses API via Sinug gateway. Native for muse-spark/gpt/grok; "
            "auto reverse-bridged through the chat pipeline for other models "
            "(mimo/deepseek/glm/kimi/claude/qwen/...). Use this tool when the "
            "caller prefers the Responses object shape, especially for muse-spark."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "model": {"type": "string", "description": "Model id from list_models."},
                "input": {"description": "Responses `input`: string or array of input items (required)."},
                "instructions": {"type": "string", "description": "Optional system instructions."},
                "max_output_tokens": {"type": "integer", "description": "Max output tokens (default 65536)."},
                "reasoning_effort": {"type": "string", "description": "Reasoning effort; muse-spark is always forced to xhigh."},
                "temperature": {"type": "number"},
                "tools": {
                    "type": "array",
                    "description": "Optional function tools (OpenAI or MCP inputSchema shape).",
                    "items": {"type": "object"},
                },
            },
            "required": ["model", "input"],
        },
    },
    {
        "name": "list_models",
        "description": "List live free models advertised by upstream (id, native endpoint, context window). No inference call.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "filter": {"type": "string", "description": "Optional case-insensitive substring filter on model id."},
            },
        },
    },
    {
        "name": "gateway_props",
        "description": "Read-only gateway config snapshot (version, relay pool, egress order, MCP tools). No upstream call.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


def _rpc_ok(rpc_id: Any, result: Any) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": rpc_id, "result": result}


def _rpc_err(rpc_id: Any, code: int, message: str, data: Any = None) -> Dict[str, Any]:
    err: Dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": rpc_id, "error": err}


def _negotiate_protocol(client_version: Any) -> str:
    if isinstance(client_version, str) and client_version in MCP_PROTOCOL_VERSIONS:
        return client_version
    return MCP_LATEST


def _default_use_relay() -> bool:
    try:
        from app.services.relay_store import get_effective_use_relay
        return bool(get_effective_use_relay())
    except (ImportError, AttributeError, TypeError, ValueError):
        try:
            from app.core.config import USE_RELAY
            return bool(USE_RELAY)
        except (ImportError, AttributeError, TypeError, ValueError):
            return True


def _mcp_headers() -> Dict[str, str]:
    try:
        from app.services.opencode import _resolve_opencode_headers
        return _resolve_opencode_headers({})
    except (ImportError, AttributeError, TypeError, ValueError):
        return {}


# ── Tool implementations (reuse gateway pipelines, buffered non-stream) ──

async def _tool_chat(args: Dict[str, Any]) -> Dict[str, Any]:
    """Jalankan inferensi chat (semua model, spark via bridge)."""
    from fastapi import BackgroundTasks as _BT
    from app.core.schemas import ChatCompletionRequest, ChatMessage

    model = (args.get("model") or "").strip() if isinstance(args.get("model"), str) else ""
    if not model:
        raise ValueError("`model` is required (see list_models)")
    raw_messages = args.get("messages")
    prompt = args.get("prompt")
    system = args.get("system")
    messages: List[ChatMessage] = []
    if isinstance(system, str) and system.strip():
        messages.append(ChatMessage(role="system", content=system))
    if isinstance(raw_messages, list) and raw_messages:
        for item in raw_messages:
            if isinstance(item, str):
                messages.append(ChatMessage(role="user", content=item))
            elif isinstance(item, dict):
                role = str(item.get("role", "user"))
                msg = ChatMessage(role=role, content=item.get("content"))
                if isinstance(item.get("tool_calls"), list):
                    msg.tool_calls = item["tool_calls"]
                if isinstance(item.get("tool_call_id"), str):
                    msg.tool_call_id = item["tool_call_id"]
                # Pertahankan extra (name, dsb.) yang valid upstream.
                try:
                    for _k in ("name",):
                        if item.get(_k) is not None and getattr(msg, "model_extra", None) is not None:
                            msg.model_extra[_k] = item[_k]  # type: ignore[index]
                except (AttributeError, TypeError, ValueError):
                    pass
                messages.append(msg)
    elif isinstance(prompt, str) and prompt.strip():
        messages.append(ChatMessage(role="user", content=prompt))
    else:
        raise ValueError("either `messages` (non-empty array) or `prompt` (non-empty string) is required")

    def _num(key: str) -> Optional[float]:
        val = args.get(key)
        return val if isinstance(val, (int, float)) and not isinstance(val, bool) else None

    temperature = _num("temperature")
    max_tokens_raw = args.get("max_tokens")
    max_tokens = int(max_tokens_raw) if isinstance(max_tokens_raw, (int, float)) and not isinstance(max_tokens_raw, bool) else 65536
    tools = args.get("tools") if isinstance(args.get("tools"), list) else None
    tool_choice = args.get("tool_choice")

    req = ChatCompletionRequest(
        model=model,
        messages=messages,
        temperature=float(temperature) if temperature is not None else 0.7,
        max_tokens=max_tokens,
        stream=False,
        tools=tools,
        tool_choice=tool_choice,
    )
    try:
        from app.core.config import _is_responses_only_model as _is_spark
        is_spark = bool(_is_spark(model))
    except (ImportError, AttributeError, TypeError, ValueError):
        is_spark = "muse-spark" in model.lower()

    background_tasks = _BT()
    oc_headers = _mcp_headers()
    try:
        from app.services.opencode import _stable_opencode_session
        oc_headers = dict(oc_headers)
        oc_headers["x-opencode-session"] = _stable_opencode_session(
            {"messages": [m.to_upstream() for m in messages]}
        )
    except (ImportError, AttributeError, TypeError, ValueError):
        pass
    use_relay = _default_use_relay()

    if is_spark:
        from app.services.responses_bridge import build_responses_payload_from_chat, responses_to_chat_stream_generator
        from app.services.collect import collect_chat_completion
        responses_payload = build_responses_payload_from_chat(req)
        wire_payload = dict(responses_payload)
        wire_payload["stream"] = True
        try:
            content, tool_calls, _finish, usage = await collect_chat_completion(
                lambda: responses_to_chat_stream_generator(
                    wire_payload, client_model=model, include_usage_requested=True,
                    background_tasks=background_tasks, use_relay=bool(use_relay),
                    opencode_headers=oc_headers,
                ),
                client_model=model,
            )
        except Exception as exc:  # noqa: BLE001 - ubah ke tool error MCP
            from app.core.errors import UpstreamEmptyResponse as _Empty
            if isinstance(exc, _Empty):
                content, tool_calls = "", []
                usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            else:
                raise
    else:
        from app.services.upstream import build_upstream_payload
        from app.services.streaming import stream_generator
        from app.services.collect import collect_chat_completion
        payload = build_upstream_payload(req)
        wire_payload = dict(payload)
        wire_payload["stream"] = True
        wire_stream_opts = wire_payload.get("stream_options")
        if not isinstance(wire_stream_opts, dict):
            wire_stream_opts = {}
        wire_stream_opts["include_usage"] = True
        wire_payload["stream_options"] = wire_stream_opts
        content, tool_calls, finish_reason, usage = await collect_chat_completion(
            lambda: stream_generator(
                wire_payload, client_model=model, include_usage_requested=True,
                background_tasks=background_tasks, use_relay=bool(use_relay),
                opencode_headers=oc_headers,
            ),
            client_model=model,
        )
    try:
        await background_tasks()  # catat usage SQLite (best-effort)
    except (TypeError, ValueError, AttributeError, RuntimeError):
        pass
    return {"content": content, "tool_calls": tool_calls, "usage": usage, "model": model}


async def _tool_responses(args: Dict[str, Any]) -> Dict[str, Any]:
    """Jalankan Responses API (native untuk spark; reverse-bridge untuk lainnya)."""
    from fastapi import BackgroundTasks as _BT
    model = (args.get("model") or "").strip() if isinstance(args.get("model"), str) else ""
    if not model:
        raise ValueError("`model` is required (see list_models)")
    if "input" not in args:
        raise ValueError("`input` is required (string or array of input items)")
    body: Dict[str, Any] = {"model": model, "input": args["input"]}
    for key in ("instructions", "system", "temperature", "top_p", "reasoning",
                "max_output_tokens", "max_completion_tokens", "max_tokens", "tools",
                "tool_choice", "parallel_tool_calls", "prompt_cache_key"):
        if args.get(key) is not None:
            body[key] = args[key]
    if isinstance(args.get("reasoning_effort"), str) and not isinstance(body.get("reasoning"), dict):
        body["reasoning"] = {"effort": args["reasoning_effort"]}
    try:
        from app.services.opencode import ensure_responses_wire_fields
        ensure_responses_wire_fields(body)
    except (ImportError, AttributeError, TypeError, ValueError):
        body.setdefault("store", False)
    try:
        from app.services.model_endpoints import is_responses_native
        native = bool(is_responses_native(model))
    except (ImportError, AttributeError, TypeError, ValueError):
        native = "muse-spark" in model.lower()

    background_tasks = _BT()
    oc_headers = _mcp_headers()
    try:
        from app.services.opencode import _stable_opencode_session
        oc_headers = dict(oc_headers)
        oc_headers["x-opencode-session"] = _stable_opencode_session(body)
    except (ImportError, AttributeError, TypeError, ValueError):
        pass
    use_relay = _default_use_relay()

    if native:
        from app.services.responses_bridge import responses_stream_generator
        from app.services.collect import collect_responses_object
        wire_body = dict(body)
        wire_body["stream"] = True
        result = await collect_responses_object(
            lambda: responses_stream_generator(
                wire_body, client_model=model, background_tasks=background_tasks,
                use_relay=bool(use_relay), opencode_headers=oc_headers,
            ),
            client_model=model,
        )
    else:
        from app.services.chat_bridge import build_chat_request_from_responses, chat_completion_to_responses
        from app.services.upstream import build_upstream_payload
        from app.services.streaming import stream_generator
        from app.services.collect import collect_chat_completion
        chat_req = build_chat_request_from_responses(dict(body), model)
        payload = build_upstream_payload(chat_req)
        wire_payload = dict(payload)
        wire_payload["stream"] = True
        wire_stream_opts = wire_payload.get("stream_options")
        if not isinstance(wire_stream_opts, dict):
            wire_stream_opts = {}
        wire_stream_opts["include_usage"] = True
        wire_payload["stream_options"] = wire_stream_opts
        content, tool_calls, _finish, usage = await collect_chat_completion(
            lambda: stream_generator(
                wire_payload, client_model=model, include_usage_requested=True,
                background_tasks=background_tasks, use_relay=bool(use_relay),
                opencode_headers=oc_headers,
            ),
            client_model=model,
        )
        result = chat_completion_to_responses(
            {"choices": [{"message": {"role": "assistant", "content": content, **({"tool_calls": tool_calls} if tool_calls else {})}}], "usage": usage},
            model,
        )
    try:
        await background_tasks()
    except (TypeError, ValueError, AttributeError, RuntimeError):
        pass
    # Ringkas output untuk MCP content text.
    texts: List[str] = []
    try:
        for item in result.get("output", []) or []:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "message":
                for part in item.get("content", []) or []:
                    if isinstance(part, dict) and part.get("type") in ("output_text", "text"):
                        text = part.get("text", "")
                        if isinstance(text, str) and text:
                            texts.append(text)
    except (AttributeError, TypeError, ValueError):
        pass
    return {"output_text": "".join(texts), "response": result, "model": model}


async def _tool_list_models(args: Dict[str, Any]) -> Dict[str, Any]:
    from app.services.models_cache import _fetch_opencode_free_models
    models = await _fetch_opencode_free_models()
    filt = args.get("filter") if isinstance(args.get("filter"), str) else ""
    filt = filt.strip().lower()
    out = []
    for m in models:
        try:
            mid = m.id
        except AttributeError:
            continue
        if filt and filt not in mid.lower():
            continue
        try:
            extra = m.model_dump() if hasattr(m, "model_dump") else {}
        except (TypeError, ValueError, AttributeError):
            extra = {}
        out.append({
            "id": mid,
            "endpoint": extra.get("endpoint"),
            "context_length": extra.get("context_length"),
            "modalities": extra.get("modalities", ["text"]),
        })
    return {"models": out, "count": len(out)}


async def _tool_gateway_props(_args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        from app.core.config import APP_VERSION, MODEL, REQUEST_TIMEOUT, USE_RELAY, RELAY_FALLBACK, RELAY_URLS
        version, model, timeout = str(APP_VERSION), MODEL, int(REQUEST_TIMEOUT)
        urls, use, fb = list(RELAY_URLS), bool(USE_RELAY), bool(RELAY_FALLBACK)
    except (ImportError, AttributeError, TypeError, ValueError):
        version, model, timeout, urls, use, fb = "2.0.0", "", 120, [], True, True
    try:
        from app.services.relay_store import get_effective_relays, get_effective_use_relay, get_effective_fallback, get_egress_order
        urls, use, fb, order = list(get_effective_relays()), bool(get_effective_use_relay()), bool(get_effective_fallback()), str(get_egress_order())
    except (ImportError, AttributeError, TypeError, ValueError):
        order = "relay_first"
    return {
        "name": "sinug-gateway", "version": version,
        "model": model or "(selected per request)", "request_timeout": timeout,
        "relay": {"urls": urls, "enabled": use, "fallback": fb},
        "egress_order": order, "mcp": {"protocol_versions": list(MCP_PROTOCOL_VERSIONS), "tools": [t["name"] for t in _MCP_TOOLS]},
    }


_TOOL_HANDLERS = {
    "chat": _tool_chat,
    "responses": _tool_responses,
    "list_models": _tool_list_models,
    "gateway_props": _tool_gateway_props,
}


def _tool_result_text(name: str, data: Dict[str, Any]) -> str:
    try:
        if name == "chat":
            text = data.get("content", "")
            calls = data.get("tool_calls", []) or []
            if calls:
                names = ", ".join(str(c.get("function", {}).get("name", "?")) for c in calls if isinstance(c, dict))
                return f"{text}\n[tool_calls: {names}]" if text else f"[tool_calls: {names}]"
            return text if isinstance(text, str) else json.dumps(data, ensure_ascii=False)[:4000]
        if name == "responses":
            text = data.get("output_text", "")
            return text if isinstance(text, str) else json.dumps(data, ensure_ascii=False)[:4000]
        return json.dumps(data, ensure_ascii=False)[:4000]
    except (TypeError, ValueError, AttributeError):
        return str(data)[:4000]


async def _dispatch_one(req: Any) -> Optional[Dict[str, Any]]:
    """Dispatch satu objek JSON-RPC. Return None untuk notifikasi/invalid tanpa id."""
    if not isinstance(req, dict):
        return _rpc_err(None, -32600, "Invalid Request: object expected")
    rpc_id = req.get("id")
    method = req.get("method")
    is_notification = "id" not in req
    if not isinstance(method, str):
        if is_notification:
            return None
        return _rpc_err(rpc_id, -32600, "Invalid Request: method must be a string")
    params = req.get("params")
    if params is None:
        params = {}
    if not isinstance(params, dict):
        if is_notification:
            return None
        return _rpc_err(rpc_id, -32602, "Invalid params: object expected")

    if method == "initialize":
        result = {
            "protocolVersion": _negotiate_protocol(params.get("protocolVersion")),
            "capabilities": {"tools": {"listChanged": False}, "prompts": {}, "resources": {}},
            "serverInfo": _server_info(),
        }
        return None if is_notification else _rpc_ok(rpc_id, result)
    if method in ("notifications/initialized", "notifications/cancelled"):
        return None
    if method == "ping":
        return None if is_notification else _rpc_ok(rpc_id, {})
    if method == "tools/list":
        return None if is_notification else _rpc_ok(rpc_id, {"tools": _MCP_TOOLS})
    if method == "tools/call":
        if is_notification:
            return None
        name = params.get("name")
        args = params.get("arguments")
        if not isinstance(name, str) or not name:
            return _rpc_err(rpc_id, -32602, "Invalid params: `name` (string) required")
        if args is None:
            args = {}
        if not isinstance(args, dict):
            return _rpc_err(rpc_id, -32602, "Invalid params: `arguments` (object) required")
        handler = _TOOL_HANDLERS.get(name)
        if handler is None:
            return _rpc_err(rpc_id, -32602, f"Unknown tool: {name}", {"available": sorted(_TOOL_HANDLERS)})
        try:
            data = await handler(args)
        except ValueError as exc:
            return _rpc_err(rpc_id, -32602, f"Invalid params: {exc}")
        except Exception as exc:  # noqa: BLE001 - jadi tool error, bukan crash
            text = f"{type(exc).__name__}: {exc}"[:2000]
            return _rpc_ok(rpc_id, {"content": [{"type": "text", "text": text}], "isError": True})
        return _rpc_ok(rpc_id, {
            "content": [{"type": "text", "text": _tool_result_text(name, data)}],
            "structuredContent": data,
            "isError": False,
        })
    if method == "prompts/list":
        return None if is_notification else _rpc_ok(rpc_id, {"prompts": []})
    if method == "resources/list":
        return None if is_notification else _rpc_ok(rpc_id, {"resources": []})
    if method == "resources/templates/list":
        return None if is_notification else _rpc_ok(rpc_id, {"resourceTemplates": []})
    if method == "completion/complete":
        return None if is_notification else _rpc_ok(rpc_id, {"completion": {"values": []}})
    if method.startswith("notifications/"):
        return None
    if is_notification:
        return None
    return _rpc_err(rpc_id, -32601, f"Method not found: {method}")


@router.post("/mcp", dependencies=[Depends(verify_gateway_key)])
async def mcp_streamable(request: Request):
    """JSON-RPC Streamable HTTP utama (single + batch, notifikasi -> 202)."""
    try:
        body = await request.json()
    except (ValueError, TypeError):
        return JSONResponse(content=_rpc_err(None, -32700, "Parse error: invalid JSON"), status_code=200)
    session_id = request.headers.get("mcp-session-id", "")
    headers = {"MCP-Protocol-Version": MCP_LATEST}
    if session_id:
        headers["mcp-session-id"] = session_id
    if isinstance(body, list):
        if not body:
            return JSONResponse(content=_rpc_err(None, -32600, "Invalid Request: empty batch"), status_code=200, headers=headers)
        responses = []
        for item in body:
            resp = await _dispatch_one(item)
            if resp is not None:
                responses.append(resp)
        if not responses:
            return Response(status_code=202, headers=headers)
        return JSONResponse(content=responses, headers=headers)
    resp = await _dispatch_one(body)
    if resp is None:
        return Response(status_code=202, headers=headers)
    return JSONResponse(content=resp, headers=headers)


@router.get("/mcp", dependencies=[Depends(verify_gateway_key)])
async def mcp_discovery():
    """Discovery ramah curl/browser (GET tidak hanging seperti SSE)."""
    try:
        from app.services.relay_store import get_egress_order as _get_order
        order = str(_get_order())
    except (ImportError, AttributeError, TypeError, ValueError):
        order = "relay_first"
    return {
        "name": "sinug-gateway",
        "protocol": "mcp",
        "protocolVersions": list(MCP_PROTOCOL_VERSIONS),
        "transport": "streamable-http",
        "endpoint": "/mcp",
        "legacy": {"sse": "/sse", "messages": "/messages"},
        "serverInfo": _server_info(),
        "capabilities": {"tools": {"listChanged": False}, "prompts": {}, "resources": {}},
        "tools": [{"name": t["name"], "description": t["description"]} for t in _MCP_TOOLS],
        "egress_order": order,
        "usage": "POST JSON-RPC to /mcp (initialize -> notifications/initialized -> tools/list -> tools/call)",
    }


@router.delete("/mcp", dependencies=[Depends(verify_gateway_key)])
async def mcp_delete():
    """Terminasi sesi (stateless: selalu OK)."""
    return {"ok": True, "message": "stateless MCP server: no session to terminate"}


# ── Legacy SSE transport (2024-11-05) best-effort ──

@router.get("/sse", dependencies=[Depends(verify_gateway_key)])
async def mcp_sse_legacy():
    """SSE legacy: kirim event `endpoint`, lalu komentar keepalive singkat."""
    session_id = f"sess-{secrets.token_hex(8)}"

    async def _gen():
        yield f"event: endpoint\ndata: /messages?session_id={session_id}\n\n"
        yield ": connected\n\n"
    return StreamingResponse(_gen(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache, no-transform", "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    })


@router.post("/messages", dependencies=[Depends(verify_gateway_key)])
async def mcp_messages_legacy(request: Request):
    """Terima JSON-RPC legacy; balas JSON langsung (diterima mayoritas klien)."""
    try:
        body = await request.json()
    except (ValueError, TypeError):
        return JSONResponse(content=_rpc_err(None, -32700, "Parse error: invalid JSON"), status_code=200)
    if isinstance(body, list):
        responses = []
        for item in body:
            resp = await _dispatch_one(item)
            if resp is not None:
                responses.append(resp)
        if not responses:
            return Response(status_code=202)
        return JSONResponse(content=responses)
    resp = await _dispatch_one(body)
    if resp is None:
        return Response(status_code=202)
    return JSONResponse(content=resp)


@router.get("/.well-known/mcp", dependencies=[Depends(verify_gateway_key)])
async def mcp_well_known():
    """Well-known discovery untuk klien yang auto-probe (opsional)."""
    info = await mcp_discovery()
    return info if isinstance(info, dict) else {}

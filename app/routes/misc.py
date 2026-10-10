"""Route group: misc."""
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
from app.core.config import HERMES_COMPAT, MODEL, RELAY_FALLBACK, RELAY_URLS, REQUEST_TIMEOUT, USE_RELAY


def _eff_relay_snapshot():
    """Snapshot relay efektif (website) + fallback ke env. Tak melempar."""
    try:
        from app.services.relay_store import (
            get_effective_fallback,
            get_effective_relays,
            get_effective_use_relay,
        )

        return list(get_effective_relays()), bool(get_effective_use_relay()), bool(get_effective_fallback())
    except (ImportError, AttributeError, TypeError, ValueError):
        return list(RELAY_URLS), bool(USE_RELAY), bool(RELAY_FALLBACK)
from app.services.models_cache import _enrich_model, _fetch_opencode_free_models
from app.services.relay import test_relay_connection
from app.core.schemas import HealthResponse, ModelList, PropsCapability, PropsDefaults, PropsEndpoint, PropsInfo, PropsRelay, RelayStatus


def _proxy_overview() -> Dict[str, Any]:
    """Ringkasan proxy tanpa import cycle di level modul (lazy)."""
    try:
        from app.services.outbound_proxy import get_proxy_overview
        return get_proxy_overview()
    except (ImportError, AttributeError, TypeError, ValueError):
        return {"enabled_global": False, "proxies": []}

router = APIRouter()

# Favicon: marka gerbang amber di atas charcoal (sama dengan logo sidebar).
_FAVICON_SVG = (
    "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'>"
    "<rect width='64' height='64' rx='14' fill='#df9432'/>"
    "<path d='M16 44V28a16 16 0 0 1 32 0v16' fill='none' stroke='#1c1b17' "
    "stroke-width='6' stroke-linecap='round'/>"
    "<circle cx='32' cy='44' r='5' fill='#1c1b17'/></svg>"
)


@router.get("/favicon.ico")
async def favicon():
    return Response(
        content=_FAVICON_SVG,
        media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=86400"},
    )

@router.get("/health", response_model=HealthResponse)
async def health():
    _urls, _use, _fb = _eff_relay_snapshot()
    return HealthResponse(
        status="ok",
        model=MODEL or "(selected per request)",
        relay=",".join(_urls),
        relay_enabled=_use,
        hermes_compatible=HERMES_COMPAT,
        version="2.0.0",
    )


@router.get("/v1/models", response_model=ModelList, dependencies=[Depends(verify_gateway_key)])
async def list_models():
    return ModelList(data=await _fetch_opencode_free_models())


@router.get("/models", response_model=ModelList, dependencies=[Depends(verify_gateway_key)])
async def list_models_alias():
    """OpenAI-compatible model discovery without the /v1 prefix."""
    return ModelList(data=await _fetch_opencode_free_models())


@router.get("/v1/models/{model_id}", dependencies=[Depends(verify_gateway_key)])
async def get_model(model_id: str):
    # OpenAI-compatible model lookup. Echo the requested id back so clients
    # can probe arbitrary model names without inventing a default model.
    # Enriched with the canonical context window (same aliases as list).
    return _enrich_model(model_id, id=model_id, created=int(time.time()))


# Small Ollama-compatible discovery endpoints. They are harmless for Hermes
# installations that probe multiple provider styles.
@router.get("/api/tags", dependencies=[Depends(verify_gateway_key)])
async def ollama_tags():
    free_models = await _fetch_opencode_free_models()
    return {
        "models": [
            {
                "name": m.id,
                "model": m.id,
                "modified_at": None,
                "size": 0,
                "context_length": m.context_length,
                "num_ctx": m.context_length,
            }
            for m in free_models
        ]
    }


@router.get("/api/v1/models", dependencies=[Depends(verify_gateway_key)])
async def ollama_v1_models():
    return ModelList(data=await _fetch_opencode_free_models())


@router.post("/api/show", dependencies=[Depends(verify_gateway_key)])
async def ollama_show():
    return {
        "modelfile": "",
        "parameters": "",
        "template": "",
        "details": {"format": "gguf", "family": "deepseek", "parameter_size": "unknown"},
    }


@router.get("/version")
async def version():
    return {"version": "2.0.0", "hermes_compatible": HERMES_COMPAT}


@router.get("/relay/status", response_model=RelayStatus, dependencies=[Depends(verify_gateway_key)])
async def relay_status():
    return await test_relay_connection()


@router.get("/v1/props", response_model=PropsInfo, dependencies=[Depends(verify_gateway_key)])
async def get_props():
    """Return configuration properties and supported capabilities.

    Read-only introspection endpoint. It does not call the upstream API
    and is safe to invoke frequently for monitoring or tooling. Answers
    include:
    - which upstream model is in use
    - whether relay is enabled and fallback is allowed
    - which OpenAI-compatible parameters are forwarded
    - which HTTP endpoints are exposed
    """
    _urls, _use, _fb = _eff_relay_snapshot()
    try:
        from app.services.relay_store import get_egress_order as _get_order

        _order = _get_order()
    except (ImportError, AttributeError, TypeError, ValueError):
        _order = "relay_first"
    return PropsInfo(
        model=MODEL or "(selected per request)",
        version="2.0.0",
        request_timeout=REQUEST_TIMEOUT,
        relay=PropsRelay(
            url=_urls[0] if _urls else "",
            enabled=_use,
            fallback=_fb,
        ),
        relay_urls=_urls,
        hermes_compatible=HERMES_COMPAT,
        usage_tracking=True,
        proxy_enabled=_proxy_overview().get("enabled_global", False),
        proxy_urls=[
            str(p.get("display") or "")
            for p in _proxy_overview().get("proxies", [])
        ],
        egress_order=_order,
        capabilities=[
            PropsCapability(
                name="chat_completions",
                supported=True,
                description="POST /v1/chat/completions with non-streaming JSON.",
            ),
            PropsCapability(
                name="streaming",
                supported=True,
                description="SSE streaming via stream=true on chat completions.",
            ),
            PropsCapability(
                name="tool_calling",
                supported=True,
                description="OpenAI tools/tool_choice with DSML fallback parsing.",
            ),
            PropsCapability(
                name="vision",
                supported=True,
                description="Chat image_url/file parts converted to Responses input_image/input_file (muse-spark).",
            ),
            PropsCapability(
                name="parallel_tool_calls",
                supported=True,
                description="Forwarded when supplied by the client.",
            ),
            PropsCapability(
                name="response_format",
                supported=True,
                description="Forwarded when supplied by the client.",
            ),
            PropsCapability(
                name="usage_tracking",
                supported=True,
                description="Token usage persisted in SQLite.",
            ),
            PropsCapability(
                name="outbound_proxy",
                supported=True,
                description="SOCKS5/HTTP egress pool untuk direct upstream (anti-429, konfigurasi via /monitor).",
            ),
            PropsCapability(
                name="mcp_server",
                supported=True,
                description="MCP Streamable HTTP: POST /mcp (JSON-RPC initialize/tools.list/tools.call), GET /mcp discovery, tools chat+responses+list_models+gateway_props (muse-spark auto-bridge).",
            ),
            PropsCapability(
                name="mcp_tool_passthrough",
                supported=True,
                description="Tools MCP (inputSchema) dinormalisasi ke OpenAI parameters di /v1/chat/completions + /v1/responses; fingerprint kuartet + tool_choice auto tetap ditegakkan.",
            ),
        ],
        supported_parameters=[
            "temperature",
            "max_tokens",
            "top_p",
            "stream",
            "stop",
            "presence_penalty",
            "frequency_penalty",
            "response_format",
            "seed",
            "n",
            "stream_options",
            "tools",
            "tool_choice",
            "parallel_tool_calls",
            "use_relay",
            "use_proxy",
        ],
        defaults=PropsDefaults(temperature=0.7, max_tokens=32768),
        endpoints=[
            PropsEndpoint(path="/health", method="GET", description="Liveness probe."),
            PropsEndpoint(path="/version", method="GET", description="Backend version."),
            PropsEndpoint(path="/v1/models", method="GET", description="List available free models from OpenCode."),
            PropsEndpoint(
                path="/models",
                method="GET",
                description="Alias for /v1/models without the /v1 prefix.",
            ),
            PropsEndpoint(
                path="/v1/models/{model_id}",
                method="GET",
                description="Get one model by id.",
            ),
            PropsEndpoint(
                path="/v1/chat/completions",
                method="POST",
                description="OpenAI-compatible chat completions.",
            ),
            PropsEndpoint(
                path="/v1/responses",
                method="POST",
                description="OpenAI Responses API pass-through (Muse Spark).",
            ),
            PropsEndpoint(
                path="/v1/usage",
                method="GET",
                description="Aggregated token usage for a period.",
            ),
            PropsEndpoint(
                path="/v1/usage/periods",
                method="GET",
                description="Period keywords accepted by /v1/usage.",
            ),
            PropsEndpoint(
                path="/v1/usage/cleanup",
                method="POST",
                description="Delete usage records older than retention.",
            ),
            PropsEndpoint(
                path="/v1/props",
                method="GET",
                description="Read-only snapshot of backend configuration.",
            ),
            PropsEndpoint(
                path="/relay/status",
                method="GET",
                description="Test relay connectivity and IP masking.",
            ),
            PropsEndpoint(
                path="/api/tags",
                method="GET",
                description="Ollama-compatible list of models.",
            ),
            PropsEndpoint(
                path="/api/v1/models",
                method="GET",
                description="Ollama-compatible /api/v1/models.",
            ),
            PropsEndpoint(
                path="/api/show",
                method="POST",
                description="Ollama-compatible /api/show stub.",
            ),
            PropsEndpoint(
                path="/mcp",
                method="POST",
                description="MCP Streamable HTTP JSON-RPC (initialize/tools.list/tools.call: chat, responses, list_models, gateway_props).",
            ),
            PropsEndpoint(
                path="/mcp",
                method="GET",
                description="MCP discovery (server info + tool list).",
            ),
            PropsEndpoint(
                path="/sse",
                method="GET",
                description="MCP legacy SSE transport (endpoint event).",
            ),
            PropsEndpoint(
                path="/messages",
                method="POST",
                description="MCP legacy messages endpoint (JSON-RPC).",
            ),
        ],
    )

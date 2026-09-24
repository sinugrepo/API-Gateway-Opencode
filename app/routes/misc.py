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
from app.services.models_cache import _enrich_model, _fetch_opencode_free_models
from app.services.relay import test_relay_connection
from app.core.schemas import HealthResponse, ModelList, PropsCapability, PropsDefaults, PropsEndpoint, PropsInfo, PropsRelay, RelayStatus

router = APIRouter()

# Favicon SG (sama dengan <link rel="icon"> inline di template): disajikan
# via /favicon.ico agar request otomatis browser tidak 404 dan tidak
# mengotori log. SVG modern didukung semua browser terkini sebagai favicon.
_FAVICON_SVG = (
    "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'>"
    "<defs><linearGradient id='g' x1='0' y1='0' x2='1' y2='1'>"
    "<stop offset='0' stop-color='#5794f2'/>"
    "<stop offset='1' stop-color='#3d71d9'/>"
    "</linearGradient></defs>"
    "<rect width='64' height='64' rx='14' fill='url(#g)'/>"
    "<text x='32' y='43' font-family='system-ui,sans-serif' font-size='26' "
    "font-weight='700' fill='white' text-anchor='middle'>SG</text></svg>"
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
    return HealthResponse(
        status="ok",
        model=MODEL or "(selected per request)",
        relay=",".join(RELAY_URLS),
        relay_enabled=USE_RELAY,
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
    return PropsInfo(
        model=MODEL or "(selected per request)",
        version="2.0.0",
        request_timeout=REQUEST_TIMEOUT,
        relay=PropsRelay(
            url=RELAY_URLS[0],
            enabled=USE_RELAY,
            fallback=RELAY_FALLBACK,
        ),
        relay_urls=RELAY_URLS,
        hermes_compatible=HERMES_COMPAT,
        usage_tracking=True,
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
        ],
    )

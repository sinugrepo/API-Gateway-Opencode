"""Live OpenCode free-model discovery with TTL cache."""
import json
import threading
import time
from typing import List, Optional, Tuple

import httpx

from app.core.http_client import _get_http
from app.core.config import MODELS_CACHE_TTL_SECONDS, OPENCODE_MODELS_URL
from app.core.errors import UpstreamError
from app.core.schemas import ModelInfo
from app.services.model_context import get_model_context_window, supports_vision
from app.services.model_endpoints import get_model_endpoint
from starlette.status import (
    HTTP_502_BAD_GATEWAY,
    HTTP_503_SERVICE_UNAVAILABLE,
    HTTP_504_GATEWAY_TIMEOUT,
)


def _enrich_model(model_id: str, **fields) -> ModelInfo:
    """Build ModelInfo with canonical context window aliases.

    All four aliases (context_length / context_window / max_input_tokens /
    max_context_length) carry the same value so OpenRouter-style,
    LiteLLM-style, and generic OpenAI clients all read the right number.
    Vision diiklankan via `modalities` + `supports_vision`/`vision` agar
    klien seperti Kilo Code mengaktifkan lampiran gambar (khususnya
    muse-spark yang terverifikasi live). Extra fields lolos karena
    `ModelInfo.model_config = extra="allow"`.
    """
    ctx = get_model_context_window(model_id)
    endpoint = fields.pop("endpoint", None) or get_model_endpoint(model_id)
    vision = supports_vision(model_id)
    fields.setdefault("modalities", ["text", "image"] if vision else ["text"])
    fields.setdefault("supports_vision", vision)
    fields.setdefault("vision", vision)
    return ModelInfo(
        context_length=ctx,
        context_window=ctx,
        max_input_tokens=ctx,
        max_context_length=ctx,
        endpoint=endpoint,
        **fields,
    )


_models_cache: Optional[Tuple[float, List[ModelInfo]]] = None


_models_cache_lock = threading.Lock()


def _is_free_model(model_id: str) -> bool:
    """OpenCode marks free-tier models with a `-free` suffix."""
    return isinstance(model_id, str) and model_id.lower().endswith("-free")


def _sort_free_models(models: List[ModelInfo]) -> List[ModelInfo]:
    """Sort the live OpenCode free-model list without selecting a default."""
    return sorted(models, key=lambda model: model.id.lower())


async def _fetch_opencode_free_models() -> List[ModelInfo]:
    """Fetch and cache only the free models currently advertised by OpenCode.

    Discovery failures are returned as controlled API errors. There is no
    hard-coded model list, so the proxy never advertises stale or paid models.
    """
    global _models_cache

    # Fast path: check cache outside lock.
    if _models_cache is not None:
        cached_at, cached_models = _models_cache
        if time.time() - cached_at < MODELS_CACHE_TTL_SECONDS:
            return cached_models

    try:
        client = _get_http()
        response = await client.get(
            OPENCODE_MODELS_URL,
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        payload = response.json()
    except httpx.TimeoutException as exc:
        raise UpstreamError(
            "OpenCode model discovery timed out",
            status_code=HTTP_504_GATEWAY_TIMEOUT,
        ) from exc
    except httpx.HTTPStatusError as exc:
        raise UpstreamError(
            f"OpenCode model discovery returned HTTP {exc.response.status_code}",
            status_code=HTTP_502_BAD_GATEWAY,
            upstream_status=exc.response.status_code,
        ) from exc
    except httpx.RequestError as exc:
        raise UpstreamError(
            "OpenCode model discovery is unavailable",
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
        ) from exc
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise UpstreamError(
            "OpenCode returned an invalid model list",
            status_code=HTTP_502_BAD_GATEWAY,
        ) from exc

    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise UpstreamError(
            "OpenCode returned an invalid model list",
            status_code=HTTP_502_BAD_GATEWAY,
        )

    free_models: List[ModelInfo] = []
    for item in payload["data"]:
        if not isinstance(item, dict):
            continue
        model_id = item.get("id")
        if not _is_free_model(model_id):
            continue
        try:
            created = int(item.get("created", time.time()))
        except (TypeError, ValueError):
            created = int(time.time())
        free_models.append(
            _enrich_model(
                model_id,
                id=model_id,
                object=item.get("object", "model"),
                created=created,
                owned_by=item.get("owned_by", "opencode"),
            )
        )

    if not free_models:
        raise UpstreamError(
            "OpenCode currently advertises no free models",
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
        )

    sorted_models = _sort_free_models(free_models)
    with _models_cache_lock:
        if _models_cache is None or time.time() - _models_cache[0] >= MODELS_CACHE_TTL_SECONDS:
            _models_cache = (time.time(), sorted_models)
    return sorted_models

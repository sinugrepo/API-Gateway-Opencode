"""Native upstream endpoint category per OpenCode Zen model.

Background: every Zen model is natively served on exactly ONE upstream
protocol (https://opencode.ai/docs/zen — table "Endpoints"):

- ``responses``: ``.../zen/v1/responses``   (muse-spark, gpt-*, grok-*)
- ``messages``:  ``.../zen/v1/messages``    (claude-*, qwen*)
- ``chat``:      ``.../zen/v1/chat/completions`` (deepseek, minimax, glm,
  kimi, mimo, ling, nemotron, big-pickle, jev, ...)

Sending a model to the wrong protocol fails upstream (verified live:
``mimo-v2.6-flash-free`` via ``/v1/responses`` -> 500 on relay AND direct).
The proxy therefore resolves each model's native category and:

- ``/v1/chat/completions`` keeps its current behavior (bridge only for
  Responses-only models — see ``RESPONSES_ONLY_MODELS``);
- ``/v1/responses`` serves ``responses``-category models by pass-through
  and reverse-bridges every other category through the chat pipeline
  (``app.services.chat_bridge``), so ANY model works on EITHER proxy
  endpoint without the client knowing upstream topology.

Override without code change:
    MODEL_ENDPOINT_OVERRIDES_JSON='{"my-model": "responses", "qwen*": "messages"}'
Values: ``chat`` | ``responses`` | ``messages``. Supports exact ids
(case-insensitive) and ``prefix*`` wildcards.
"""
from __future__ import annotations

import json
import os
from typing import Dict, Optional

CHAT = "chat"
RESPONSES = "responses"
MESSAGES = "messages"

_VALID = frozenset({CHAT, RESPONSES, MESSAGES})

# Exact per-model categories (lowercase id -> category). Covers every id
# advertised by upstream on 2026-09-22/23, mirroring the official docs table.
MODEL_ENDPOINTS: Dict[str, str] = {
    # ── Responses-native (verified live via /v1/responses) ──
    "muse-spark-1.3": RESPONSES,
    "muse-spark-1.2": RESPONSES,
    "muse-spark-1.3-contributor-free": RESPONSES,
    "muse-spark-1.2-contributor-free": RESPONSES,
    "gpt-5": RESPONSES,
    "gpt-5-codex": RESPONSES,
    "gpt-5-nano": RESPONSES,
    "gpt-5.1": RESPONSES,
    "gpt-5.1-codex": RESPONSES,
    "gpt-5.1-codex-max": RESPONSES,
    "gpt-5.1-codex-mini": RESPONSES,
    "gpt-5.2": RESPONSES,
    "gpt-5.2-codex": RESPONSES,
    "gpt-5.3-codex": RESPONSES,
    "gpt-5.3-codex-spark": RESPONSES,
    "gpt-5.4": RESPONSES,
    "gpt-5.4-mini": RESPONSES,
    "gpt-5.4-nano": RESPONSES,
    "gpt-5.4-pro": RESPONSES,
    "gpt-5.5": RESPONSES,
    "gpt-5.5-pro": RESPONSES,
    "gpt-5.6-luna": RESPONSES,
    "gpt-5.6-sol": RESPONSES,
    "gpt-5.6-terra": RESPONSES,
    "gpt-6-astra": RESPONSES,
    "grok-4.5": RESPONSES,
    "grok-4.6": RESPONSES,
    "grok-4.7": RESPONSES,
    "grok-build-0.1": RESPONSES,
    # ── Messages-native (Anthropic-style; docs table) ──
    "claude-fable-5": MESSAGES,
    "claude-fable-5-1": MESSAGES,
    "claude-opus-5": MESSAGES,
    "claude-opus-4-5": MESSAGES,
    "claude-opus-4-6": MESSAGES,
    "claude-opus-4-7": MESSAGES,
    "claude-opus-4-8": MESSAGES,
    "claude-sonnet-4": MESSAGES,
    "claude-sonnet-4-5": MESSAGES,
    "claude-sonnet-4-6": MESSAGES,
    "claude-sonnet-5": MESSAGES,
    "claude-haiku-4-5": MESSAGES,
    "qwen3.5-plus": MESSAGES,
    "qwen3.6-plus": MESSAGES,
    "qwen3.8-flash": MESSAGES,
    # ── Chat-native (OpenAI-compatible; verified live for mimo/spark-free) ──
    "deepseek-v4-flash": CHAT,
    "deepseek-v4-flash-free": CHAT,
    "deepseek-v4-flash-vision-exp": CHAT,
    "deepseek-v4-pro": CHAT,
    "deepseek-v4.1-flash": CHAT,
    "glm-5": CHAT,
    "glm-5.1": CHAT,
    "glm-5.2": CHAT,
    "glm-5.3": CHAT,
    "glm-5.3-flash": CHAT,
    "kimi-k2.5": CHAT,
    "kimi-k2.6": CHAT,
    "kimi-k2.7-code": CHAT,
    "kimi-k3": CHAT,
    "minimax-m2.5": CHAT,
    "minimax-m2.7": CHAT,
    "minimax-m3": CHAT,
    "big-pickle": CHAT,
    "mimo-v2.5-free": CHAT,
    "mimo-v2.6-flash-free": CHAT,
    "ling-3.0-flash-fin-free": CHAT,
    "nemotron-3-ultra-free": CHAT,
    "nemotron-3.5-lightning-free": CHAT,
    "jev-1.13": CHAT,
    "jev-1.13-free": CHAT,
}

# Prefix fallback for ids not yet in the exact table (longest match wins).
# NOTE: "gpt-" must stay RESPONSES only for the documented gpt family;
# unknown future families default to CHAT (widest-supported protocol).
PREFIX_ENDPOINTS: Dict[str, str] = {
    "muse-spark": RESPONSES,
    "gpt-": RESPONSES,
    "grok-": RESPONSES,
    "claude-": MESSAGES,
    "qwen": MESSAGES,
    "deepseek-": CHAT,
    "glm-": CHAT,
    "kimi-": CHAT,
    "minimax-": CHAT,
    "mimo-": CHAT,
    "ling-": CHAT,
    "nemotron-": CHAT,
    "jev-": CHAT,
    "big-pickle": CHAT,
}

# Fallback for completely unknown ids: chat protocol is the widest-supported
# (all current non-responses/non-messages families speak it).
DEFAULT_ENDPOINT = CHAT


def _load_env_overrides() -> Dict[str, str]:
    raw = os.getenv("MODEL_ENDPOINT_OVERRIDES_JSON", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[str, str] = {}
    for key, value in data.items():
        if (
            isinstance(key, str)
            and key.strip()
            and isinstance(value, str)
            and value.strip().lower() in _VALID
        ):
            out[key.strip().lower()] = value.strip().lower()
    return out


def get_model_endpoint(model_id: Optional[str]) -> str:
    """Return native upstream category for any model id.

    Resolution order: env exact override -> env prefix* override ->
    exact table -> prefix table (longest match) -> ``chat`` default.
    Env wins over built-ins so operators can correct any entry without
    code changes.
    """
    name = (model_id or "").strip().lower()
    if not name:
        return DEFAULT_ENDPOINT
    env = _load_env_overrides()
    if name in env:
        return env[name]
    best_env: Optional[str] = None
    best_env_len = -1
    for key, category in env.items():
        if key.endswith("*"):
            prefix = key[:-1]
            if prefix and name.startswith(prefix) and len(prefix) > best_env_len:
                best_env = category
                best_env_len = len(prefix)
    if best_env is not None:
        return best_env
    if name in MODEL_ENDPOINTS:
        return MODEL_ENDPOINTS[name]
    best: Optional[str] = None
    best_len = -1
    for prefix, category in PREFIX_ENDPOINTS.items():
        if name.startswith(prefix) and len(prefix) > best_len:
            best = category
            best_len = len(prefix)
    if best is not None:
        return best
    return DEFAULT_ENDPOINT


def is_responses_native(model_id: Optional[str]) -> bool:
    """True when the model must be served via Responses API upstream."""
    try:
        return get_model_endpoint(model_id) == RESPONSES
    except (TypeError, ValueError, AttributeError):
        return False

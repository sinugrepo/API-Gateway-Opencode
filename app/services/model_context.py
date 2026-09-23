"""Canonical context-window table for OpenCode Zen models.

Upstream ``GET /zen/v1/models`` only returns ``id/object/created/owned_by``
— no context length. Clients (Hermes, Kilo, OpenAI SDK) rely on the
proxy's ``/v1/models`` to know how much context they can send, so the
proxy enriches every entry with a verified window.

Source of truth per family (checked 2026-09-22):
- muse-spark 1.1/1.2/1.3 (+ contributor): 1,048,576 — https://dev.meta.ai/docs/models
  ("Context window 1,048,576 tokens" for every Muse Spark variant).
- gpt-5.x family: 400,000 — OpenAI GPT-5 spec (400K input). Used for all
  ``gpt-5*``; ``gpt-6-astra`` keeps 400K minimum until OpenAI publishes more.
  Billing threshold "≤272K / >272K" on opencode.ai/docs/zen confirms >272K usable.
- claude 4.5+/5/fable: 1,000,000 max (200K default, 1M extended) — Anthropic
  extended-thinking docs; pricing tiers ">200K tokens" confirm >200K usable.
  Older ``claude-sonnet-4`` / ``claude-haiku-4-5``: 200,000 default.
- gemini-3.x: 1,048,576 — Google Gemini 1M context.
- grok-4.x: 2,097,152 (2M) — xAI Grok-4 spec.
- qwen3.x: 262,144 (256K) — Alibaba Qwen3 spec.
- deepseek-v4*: 131,072 (128K) — DeepSeek V3/V4 spec.
- glm-5*: 200,000 — Zhipu GLM family (200K class).
- kimi-k*: 262,144 (256K) — Moonshot Kimi K2 spec.
- minimax-m*: 200,000 — MiniMax M2/M3 class.
- mimo / ling / nemotron / big-pickle / jev: best-effort vendor class
  (see table); override via env when vendors publish exact numbers.

Override without code change:
    MODEL_CONTEXT_OVERRIDES_JSON='{"my-model": 500000, "qwen*": 1000000}'
Supports exact ids (case-insensitive) and ``prefix*`` wildcards.
"""
from __future__ import annotations

import json
import os
from typing import Dict, Optional

# Verified anchor: Muse Spark family — 1M (Meta docs).
MUSE_SPARK_CONTEXT = 1_048_576

# Exact per-model overrides (lowercase id -> tokens). Covers every id
# advertised by upstream on 2026-09-22 (76 ids) so /v1/models never falls
# back to the generic default for a known model.
MODEL_CONTEXT_WINDOWS: Dict[str, int] = {
    # ── Muse Spark (verified 1,048,576) ──
    "muse-spark-1.3": MUSE_SPARK_CONTEXT,
    "muse-spark-1.2": MUSE_SPARK_CONTEXT,
    "muse-spark-1.3-contributor-free": MUSE_SPARK_CONTEXT,
    "muse-spark-1.2-contributor-free": MUSE_SPARK_CONTEXT,
    # ── GPT (OpenAI 400K class) ──
    "gpt-5": 400_000,
    "gpt-5-codex": 400_000,
    "gpt-5-nano": 400_000,
    "gpt-5.1": 400_000,
    "gpt-5.1-codex": 400_000,
    "gpt-5.1-codex-max": 400_000,
    "gpt-5.1-codex-mini": 400_000,
    "gpt-5.2": 400_000,
    "gpt-5.2-codex": 400_000,
    "gpt-5.3-codex": 400_000,
    "gpt-5.3-codex-spark": 400_000,
    "gpt-5.4": 400_000,
    "gpt-5.4-mini": 400_000,
    "gpt-5.4-nano": 400_000,
    "gpt-5.4-pro": 400_000,
    "gpt-5.5": 400_000,
    "gpt-5.5-pro": 400_000,
    "gpt-5.6-luna": 400_000,
    "gpt-5.6-sol": 400_000,
    "gpt-5.6-terra": 400_000,
    "gpt-6-astra": 400_000,
    # ── Claude (Anthropic; 4.5+/5/fable support 1M extended) ──
    "claude-fable-5": 1_000_000,
    "claude-fable-5-1": 1_000_000,
    "claude-opus-5": 1_000_000,
    "claude-opus-4-5": 1_000_000,
    "claude-opus-4-6": 1_000_000,
    "claude-opus-4-7": 1_000_000,
    "claude-opus-4-8": 1_000_000,
    "claude-sonnet-4": 200_000,
    "claude-sonnet-4-5": 1_000_000,
    "claude-sonnet-4-6": 1_000_000,
    "claude-sonnet-5": 1_000_000,
    "claude-haiku-4-5": 200_000,
    # ── Gemini (Google 1M class) ──
    "gemini-3-flash": MUSE_SPARK_CONTEXT,
    "gemini-3.1-pro": MUSE_SPARK_CONTEXT,
    "gemini-3.5-flash": MUSE_SPARK_CONTEXT,
    "gemini-3.5-flash-lite": MUSE_SPARK_CONTEXT,
    "gemini-3.6-flash": MUSE_SPARK_CONTEXT,
    "gemini-3.7-flash": MUSE_SPARK_CONTEXT,
    "gemini-3.8-flash": MUSE_SPARK_CONTEXT,
    # ── Grok (xAI 2M class) ──
    "grok-4.5": 2_097_152,
    "grok-4.6": 2_097_152,
    "grok-4.7": 2_097_152,
    "grok-build-0.1": 2_097_152,
    # ── Qwen (Alibaba 256K class) ──
    "qwen3.5-plus": 262_144,
    "qwen3.6-plus": 262_144,
    "qwen3.8-flash": 262_144,
    # ── DeepSeek (128K class) ──
    "deepseek-v4-flash": 131_072,
    "deepseek-v4-flash-free": 131_072,
    "deepseek-v4-flash-vision-exp": 131_072,
    "deepseek-v4-pro": 131_072,
    "deepseek-v4.1-flash": 131_072,
    # ── GLM (Zhipu 200K class) ──
    "glm-5": 200_000,
    "glm-5.1": 200_000,
    "glm-5.2": 200_000,
    "glm-5.3": 200_000,
    "glm-5.3-flash": 200_000,
    # ── Kimi (Moonshot 256K class) ──
    "kimi-k2.5": 262_144,
    "kimi-k2.6": 262_144,
    "kimi-k2.7-code": 262_144,
    "kimi-k3": 262_144,
    # ── MiniMax (200K class) ──
    "minimax-m2.5": 200_000,
    "minimax-m2.7": 200_000,
    "minimax-m3": 200_000,
    # ── Free/experimental open models (vendor class best-effort) ──
    "big-pickle": 200_000,
    "mimo-v2.5-free": 262_144,
    "mimo-v2.6-flash-free": 262_144,
    "ling-3.0-flash-fin-free": 131_072,
    "nemotron-3-ultra-free": 262_144,
    "nemotron-3.5-lightning-free": 262_144,
    "jev-1.13": 131_072,
    "jev-1.13-free": 131_072,
}

# Prefix fallback for model ids not yet in the exact table
# (e.g. future "muse-spark-1.4" or "gpt-5.7"). Longest prefix wins.
PREFIX_CONTEXT_WINDOWS: Dict[str, int] = {
    "muse-spark": MUSE_SPARK_CONTEXT,
    "gpt-": 400_000,
    "claude-opus-": 1_000_000,
    "claude-sonnet-4-": 1_000_000,
    "claude-sonnet-5": 1_000_000,
    "claude-sonnet-": 200_000,
    "claude-haiku-": 200_000,
    "claude-fable-": 1_000_000,
    "claude-": 200_000,
    "gemini-": MUSE_SPARK_CONTEXT,
    "grok-": 2_097_152,
    "qwen": 262_144,
    "deepseek-": 131_072,
    "glm-": 200_000,
    "kimi-": 262_144,
    "minimax-": 200_000,
    "mimo-": 262_144,
    "ling-": 131_072,
    "nemotron-": 262_144,
    "jev-": 131_072,
    "big-pickle": 200_000,
}

# Fallback for completely unknown ids (never 0/None — clients divide by it).
DEFAULT_CONTEXT_WINDOW = 200_000


def _load_env_overrides() -> Dict[str, int]:
    raw = os.getenv("MODEL_CONTEXT_OVERRIDES_JSON", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[str, int] = {}
    for key, value in data.items():
        try:
            tokens = int(value)
        except (TypeError, ValueError):
            continue
        if isinstance(key, str) and key.strip() and tokens > 0:
            out[key.strip().lower()] = tokens
    return out


def get_model_context_window(model_id: Optional[str]) -> int:
    """Return canonical context window (tokens) for any model id.

    Resolution order: env exact override -> env prefix* override ->
    exact table -> prefix table (longest match) -> default.
    Env wins over built-ins so operators can correct any entry without
    code changes.
    """
    name = (model_id or "").strip().lower()
    if not name:
        return DEFAULT_CONTEXT_WINDOW
    env = _load_env_overrides()
    if name in env:
        return env[name]
    # env prefix wildcards: "qwen*" etc.
    best_env: Optional[int] = None
    best_env_len = -1
    for key, tokens in env.items():
        if key.endswith("*"):
            prefix = key[:-1]
            if prefix and name.startswith(prefix) and len(prefix) > best_env_len:
                best_env = tokens
                best_env_len = len(prefix)
    if best_env is not None:
        return best_env
    if name in MODEL_CONTEXT_WINDOWS:
        return MODEL_CONTEXT_WINDOWS[name]
    best: Optional[int] = None
    best_len = -1
    for prefix, tokens in PREFIX_CONTEXT_WINDOWS.items():
        if name.startswith(prefix) and len(prefix) > best_len:
            best = tokens
            best_len = len(prefix)
    if best is not None:
        return best
    return DEFAULT_CONTEXT_WINDOW

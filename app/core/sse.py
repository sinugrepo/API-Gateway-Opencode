"""Shared SSE helper (single source for chat + Responses bridges)."""
import json
from typing import Any, Dict


def _sse(payload: Dict[str, Any]) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

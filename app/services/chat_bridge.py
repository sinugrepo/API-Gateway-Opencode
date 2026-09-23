"""Reverse bridge: serve non-Responses-native models via /v1/responses.

Background: every Zen model natively speaks ONE upstream protocol
(``app.services.model_endpoints``). Calling a chat/messages-native model
(mimo, deepseek, glm, kimi, minimax, claude, qwen, ...) through the
Responses API fails upstream with a bare 500 — on relay AND direct
(verified live 2026-09-23: ``mimo-v2.6-flash-free``).

This module translates the OTHER direction from the existing
``responses_bridge`` (chat -> Responses):

- ``build_chat_request_from_responses``: Responses body -> chat request
  (``input``/``instructions`` -> messages, ``max_output_tokens`` ->
  ``max_tokens``, flat function tools -> chat function tools).
- The translated request runs through the SAME chat pipeline
  (``chat_completions`` / ``stream_generator``), which already routes each
  model to its working upstream path — so relay rotation, 429-cooldown,
  fingerprint gates and usage recording are reused, not reimplemented.
- ``chat_completion_to_responses`` / ``chat_stream_to_responses_stream``:
  chat results back into Responses objects / SSE events.

Streaming translation is intentionally text-first: text deltas flow live as
``response.output_text.delta``; tool calls (rare on this path) are
delivered once inside the final ``response.completed`` object. Error chunks
keep the ``{"error": ...}`` + ``[DONE]`` convention used by the native
Responses generators so clients/collectors behave identically.
"""
from __future__ import annotations

import json
import secrets
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from fastapi import BackgroundTasks

from app.core.schemas import ChatCompletionRequest, ChatMessage
from app.core.sse import _sse


def _responses_text_of(part: Any) -> str:
    """Extract display text from a Responses content part."""
    if isinstance(part, str):
        return part
    if isinstance(part, dict):
        if part.get("type") in ("input_text", "text", "output_text", "refusal"):
            text = part.get("text", "")
            return text if isinstance(text, str) else str(text)
    return ""


def _responses_item_to_chat_message(item: Any) -> Optional[Dict[str, Any]]:
    """Convert one Responses `input` item to an OpenAI chat message dict.

    Returns None for items with no mappable content (reasoning items,
    function_call items without arguments context, ...). ``function_call``
    history items become assistant tool_calls so multi-turn tool flows keep
    working; ``function_call_output`` becomes role=tool messages.
    """
    if isinstance(item, str):
        return {"role": "user", "content": item} if item.strip() else None
    if not isinstance(item, dict):
        return None
    item_type = item.get("type")
    if item_type == "function_call_output":
        text = item.get("output", "")
        if isinstance(text, (dict, list)):
            text = json.dumps(text, ensure_ascii=False)
        return {
            "role": "tool",
            "content": text if isinstance(text, str) else str(text),
            "tool_call_id": item.get("call_id") or "",
        }
    if item_type == "function_call":
        arguments = item.get("arguments", "{}")
        if isinstance(arguments, (dict, list)):
            arguments = json.dumps(arguments, ensure_ascii=False)
        elif arguments is None:
            arguments = "{}"
        return {
            "role": "assistant",
            "content": "",
            "tool_calls": [{
                "id": item.get("call_id") or item.get("id") or "call_0",
                "type": "function",
                "function": {
                    "name": item.get("name") or "",
                    "arguments": str(arguments),
                },
            }],
        }
    role = item.get("role", "user")
    if role not in ("user", "assistant", "system", "developer", "tool"):
        role = "user"
    content = item.get("content")
    texts: List[str] = []
    media: List[Dict[str, Any]] = []
    if isinstance(content, str):
        texts.append(content)
    elif isinstance(content, list):
        for part in content:
            if not isinstance(part, dict):
                if isinstance(part, str):
                    texts.append(part)
                continue
            ptype = part.get("type")
            if ptype == "input_image":
                url = part.get("image_url")
                if isinstance(url, str) and url:
                    media.append({
                        "type": "image_url",
                        "image_url": {"url": url, "detail": part.get("detail") or "auto"},
                    })
            elif ptype == "input_file":
                if isinstance(part.get("file_id"), str) and part["file_id"]:
                    media.append({"type": "file", "file": {"file_id": part["file_id"]}})
                elif isinstance(part.get("file_data"), str) and part["file_data"]:
                    media.append({"type": "file", "file": {
                        "file_data": part["file_data"],
                        "filename": part.get("filename") or "file.bin",
                    }})
            else:
                text = _responses_text_of(part)
                if text:
                    texts.append(text)
    elif content is not None:
        texts.append(str(content))
    text = "".join(texts)
    if not text and not media:
        return None
    message: Dict[str, Any] = {"role": role, "content": text}
    if media:
        # Chat vision shape: text part + media parts.
        parts: List[Dict[str, Any]] = []
        if text:
            parts.append({"type": "text", "text": text})
        parts.extend(media)
        message["content"] = parts
    return message


def responses_input_to_chat_messages(
    input_: Any,
    *,
    instructions: Optional[str] = None,
    system: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Convert Responses `input` (+instructions/system) to chat messages."""
    messages: List[Dict[str, Any]] = []
    for key in (system, instructions):
        if isinstance(key, str) and key.strip():
            messages.append({"role": "system", "content": key})
    if isinstance(input_, str):
        if input_.strip():
            messages.append({"role": "user", "content": input_})
        return messages
    if isinstance(input_, list):
        for item in input_:
            message = _responses_item_to_chat_message(item)
            if message is not None:
                messages.append(message)
    return messages


def _responses_tools_to_chat_tools(tools: Any) -> Optional[List[Dict[str, Any]]]:
    """Flat Responses function tools -> chat function tools (best-effort)."""
    if not isinstance(tools, list):
        return None
    out: List[Dict[str, Any]] = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        name = tool.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        parameters = tool.get("parameters")
        if not isinstance(parameters, dict):
            parameters = {"type": "object", "properties": {}}
        out.append({
            "type": "function",
            "function": {
                "name": name.strip(),
                "description": tool.get("description") or f"Tool {name.strip()}",
                "parameters": parameters,
            },
        })
    return out or None


def build_chat_request_from_responses(
    body: Dict[str, Any],
    client_model: str,
) -> ChatCompletionRequest:
    """Build a chat request equivalent to a Responses API body."""
    messages_raw = responses_input_to_chat_messages(
        body.get("input"),
        instructions=body.get("instructions"),
        system=body.get("system"),
    )
    if not messages_raw:
        # Degenerate but valid: keep pipeline contract (messages non-empty).
        messages_raw = [{"role": "user", "content": "Hello"}]
    chat_messages = [ChatMessage(role=m.get("role", "user"), content=m.get("content"))
                     for m in messages_raw]
    # Preserve tool_calls/tool_call_id extras lost by the strict schema.
    for chat_message, raw in zip(chat_messages, messages_raw):
        if isinstance(raw.get("tool_calls"), list):
            chat_message.tool_calls = raw["tool_calls"]
        if isinstance(raw.get("tool_call_id"), str):
            chat_message.tool_call_id = raw["tool_call_id"]

    max_tokens: Optional[int] = None
    for key in ("max_output_tokens", "max_completion_tokens", "max_tokens"):
        value = body.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
            max_tokens = int(value)
            break

    tool_choice = body.get("tool_choice")
    if isinstance(tool_choice, dict):
        # Responses {"type":"function","name":..} -> chat {"type":"function","function":{...}}.
        name = tool_choice.get("name")
        if tool_choice.get("type") == "function" and isinstance(name, str):
            tool_choice = {"type": "function", "function": {"name": name}}
    elif tool_choice not in ("auto", "none", "required", None):
        tool_choice = "auto" if _responses_tools_to_chat_tools(body.get("tools")) else None

    reasoning_effort: Optional[str] = None
    reasoning = body.get("reasoning")
    if isinstance(reasoning, dict) and isinstance(reasoning.get("effort"), str):
        reasoning_effort = reasoning["effort"]

    return ChatCompletionRequest(
        model=client_model,
        messages=chat_messages,
        temperature=body.get("temperature") if isinstance(body.get("temperature"), (int, float)) else 0.7,
        max_tokens=max_tokens if max_tokens is not None else 65536,
        stream=bool(body.get("stream", False)),
        tools=_responses_tools_to_chat_tools(body.get("tools")),
        tool_choice=tool_choice,
        parallel_tool_calls=body.get("parallel_tool_calls") if isinstance(body.get("parallel_tool_calls"), bool) else None,
        top_p=body.get("top_p") if isinstance(body.get("top_p"), (int, float)) else None,
        reasoning_effort=reasoning_effort,
    )


def _new_response_id() -> str:
    return f"resp-{secrets.token_hex(12)}"


def chat_completion_to_responses(
    result: Dict[str, Any],
    client_model: str,
    *,
    response_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Convert a chat completion dict to an OpenAI Responses object."""
    try:
        choice = (result.get("choices") or [{}])[0]
    except (AttributeError, TypeError):
        choice = {}
    message = choice.get("message") if isinstance(choice, dict) else None
    if not isinstance(message, dict):
        message = {}
    content = message.get("content", "")
    if isinstance(content, list):
        # Already parts-like; flatten text defensively.
        texts = []
        for part in content:
            if isinstance(part, dict):
                text = part.get("text")
                texts.append(text if isinstance(text, str) else str(text or ""))
            elif isinstance(part, str):
                texts.append(part)
        content = "".join(texts)
    if not isinstance(content, str):
        content = "" if content is None else str(content)
    output: List[Dict[str, Any]] = []
    if content:
        output.append({
            "type": "message",
            "id": f"msg-{secrets.token_hex(8)}",
            "role": "assistant",
            "content": [{"type": "output_text", "text": content}],
        })
    tool_calls = message.get("tool_calls")
    if isinstance(tool_calls, list):
        for index, call in enumerate(tool_calls):
            if not isinstance(call, dict):
                continue
            function = call.get("function") if isinstance(call.get("function"), dict) else {}
            arguments = function.get("arguments", "{}")
            if isinstance(arguments, (dict, list)):
                arguments = json.dumps(arguments, ensure_ascii=False)
            output.append({
                "type": "function_call",
                "id": f"fc-{secrets.token_hex(8)}",
                "call_id": call.get("id") or f"call_{index}",
                "name": function.get("name") or "",
                "arguments": str(arguments if arguments is not None else "{}"),
            })
    usage = result.get("usage") if isinstance(result.get("usage"), dict) else {}
    try:
        prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
        completion_tokens = int(usage.get("completion_tokens", 0) or 0)
        total_tokens = int(usage.get("total_tokens", 0) or (prompt_tokens + completion_tokens))
    except (TypeError, ValueError):
        prompt_tokens = completion_tokens = total_tokens = 0
    return {
        "id": response_id or result.get("id") or _new_response_id(),
        "object": "response",
        "created_at": result.get("created") or int(time.time()),
        "model": client_model,
        "status": "completed",
        "output": output,
        "usage": {
            "input_tokens": prompt_tokens,
            "output_tokens": completion_tokens,
            "total_tokens": total_tokens,
        },
    }


def empty_responses_object(client_model: str) -> Dict[str, Any]:
    """Empty-but-valid Responses object (mirrors bridge empty contract)."""
    return {
        "id": _new_response_id(),
        "object": "response",
        "created_at": int(time.time()),
        "model": client_model,
        "status": "incomplete",
        "output": [],
        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
    }


async def chat_stream_to_responses_stream(
    chat_gen: AsyncIterator[str],
    *,
    client_model: str,
    response_id: Optional[str] = None,
) -> AsyncIterator[str]:
    """Translate chat SSE chunks into Responses SSE events (text-first).

    Consumes any async iterator yielding SSE frames (``data: {...}`` /
    ``data: [DONE]`` / ``:`` keepalives) as produced by ``stream_generator``.
    Text deltas stream live; tool calls + usage land in the final
    ``response.completed``. Error chunks keep the ``{"error": ...}``
    convention, then ``[DONE]``.
    """
    resp_id = response_id or _new_response_id()
    created_at = int(time.time())
    msg_id = f"msg-{secrets.token_hex(8)}"
    sent_created = False
    sent_item = False
    text_parts: List[str] = []
    calls: Dict[int, Dict[str, Any]] = {}
    finish_reason: Optional[str] = None
    usage: Optional[Dict[str, Any]] = None
    pending_error: Optional[Any] = None
    completed = False

    def _event(obj: Dict[str, Any]) -> str:
        return _sse(obj)

    async for raw in chat_gen:
        if not isinstance(raw, str) or not raw:
            continue
        # Pass keepalive comments straight through (same wire behavior).
        if raw.startswith(":"):
            yield raw if raw.endswith("\n\n") else raw + "\n\n"
            continue
        for line in raw.splitlines():
            if not line or line.startswith(":"):
                continue
            if not line.startswith("data:"):
                continue
            data = line[5:].lstrip()
            if data == "[DONE]":
                continue
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
                chunk_usage = obj.get("usage")
                if isinstance(chunk_usage, dict) and chunk_usage:
                    usage = chunk_usage
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
                if not sent_created:
                    yield _event({"type": "response.created",
                                  "response": {"id": resp_id, "object": "response",
                                               "status": "in_progress", "model": client_model}})
                    sent_created = True
                if not sent_item:
                    yield _event({"type": "response.output_item.added", "output_index": 0,
                                  "item": {"id": msg_id, "type": "message",
                                           "role": "assistant", "content": []}})
                    sent_item = True
                text_parts.append(text)
                yield _event({"type": "response.output_text.delta", "item_id": msg_id,
                              "output_index": 0, "content_index": 0, "delta": text})
            fragments = delta.get("tool_calls")
            if isinstance(fragments, list):
                for pos, frag in enumerate(fragments):
                    if not isinstance(frag, dict):
                        continue
                    try:
                        index = int(frag.get("index", pos))
                    except (TypeError, ValueError):
                        index = pos
                    entry = calls.setdefault(index, {"id": None, "name": None, "arguments": ""})
                    if frag.get("id") and not entry["id"]:
                        entry["id"] = str(frag["id"])
                    function = frag.get("function")
                    if isinstance(function, dict):
                        if isinstance(function.get("name"), str) and function["name"] and not entry["name"]:
                            entry["name"] = function["name"]
                        args = function.get("arguments")
                        if args is not None:
                            entry["arguments"] += args if isinstance(args, str) else str(args)

    if pending_error is not None and not text_parts and not calls:
        yield _event({"error": pending_error})
        yield "data: [DONE]\n\n"
        return

    full_text = "".join(text_parts)
    output: List[Dict[str, Any]] = []
    if full_text:
        output.append({"type": "message", "id": msg_id, "role": "assistant",
                       "content": [{"type": "output_text", "text": full_text}]})
    for index in sorted(calls):
        entry = calls[index]
        if not entry["name"]:
            continue
        output.append({"type": "function_call", "id": f"fc-{secrets.token_hex(8)}",
                       "call_id": entry["id"] or f"call_{index}",
                       "name": entry["name"],
                       "arguments": entry["arguments"] or "{}"})
    prompt_tokens = completion_tokens = total_tokens = 0
    if isinstance(usage, dict):
        try:
            prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
            completion_tokens = int(usage.get("completion_tokens", 0) or 0)
            total_tokens = int(usage.get("total_tokens", 0) or (prompt_tokens + completion_tokens))
        except (TypeError, ValueError):
            pass
    if not output and pending_error is not None:
        yield _event({"error": pending_error})
        yield "data: [DONE]\n\n"
        return
    completed_response = {
        "id": resp_id,
        "object": "response",
        "created_at": created_at,
        "model": client_model,
        "status": "completed" if (finish_reason or "stop") != "length" else "incomplete",
        "output": output,
        "usage": {"input_tokens": prompt_tokens, "output_tokens": completion_tokens,
                  "total_tokens": total_tokens},
    }
    yield _event({"type": "response.completed", "response": completed_response})
    yield "data: [DONE]\n\n"

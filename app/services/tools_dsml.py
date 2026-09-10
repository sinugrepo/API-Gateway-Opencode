"""DeepSeek DSML markup <-> OpenAI tool_calls conversion."""
import html
import json
import re
from typing import Any, Dict, List, Tuple

from app.core.errors import UpstreamEmptyResponse


# DeepSeek DSML uses U+FF5C fullwidth vertical bars:
# <｜｜DSML｜｜tool_calls>...</｜｜DSML｜｜tool_calls>
_DSML_BAR = "\uff5c"


_DSML_OPEN = f"<{_DSML_BAR}{_DSML_BAR}DSML{_DSML_BAR}{_DSML_BAR}"


_DSML_CLOSE = f"</{_DSML_BAR}{_DSML_BAR}DSML{_DSML_BAR}{_DSML_BAR}"


_DSML_TOOL_START = f"{_DSML_OPEN}tool_calls>"


_DSML_TOOL_END = f"{_DSML_CLOSE}tool_calls>"


_DSML_TOOL_BLOCK_RE = re.compile(
    re.escape(_DSML_TOOL_START) + r"(.*?)" + re.escape(_DSML_TOOL_END),
    flags=re.DOTALL,
)


_DSML_INVOKE_RE = re.compile(
    re.escape(_DSML_OPEN) + r'invokename="([^"]*)">(.*?)'
    + re.escape(_DSML_CLOSE) + r"invoke>",
    flags=re.DOTALL,
)


_DSML_PARAM_RE = re.compile(
    re.escape(_DSML_OPEN)
    + r'parametername="([^"]*)"string="([^"]*)">(.*?)'
    + re.escape(_DSML_CLOSE)
    + r"parameter>",
    flags=re.DOTALL,
)


_DSML_ANY_TAG_RE = re.compile(
    r"</?" + re.escape(_DSML_BAR + _DSML_BAR + "DSML" + _DSML_BAR + _DSML_BAR) + r"[^>]*>",
    flags=re.DOTALL,
)


def _new_tool_call_id(index: int) -> str:
    return f"call_{index}_{secrets.token_hex(8)}"


def _parse_parameter_value(raw_value: str, declared_string: bool) -> Any:
    value = html.unescape(raw_value).strip()
    if declared_string:
        return value

    # DSML may label values as non-string. Preserve JSON scalar types where possible.
    try:
        return json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        pass

    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    if value.lower() == "null":
        return None

    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def normalize_tool_calls(tool_calls: Any) -> List[Dict[str, Any]]:
    """Normalize complete native or parsed calls to the OpenAI function schema."""

    if not isinstance(tool_calls, list):
        return []

    normalized: List[Dict[str, Any]] = []
    for index, item in enumerate(tool_calls):
        if not isinstance(item, dict):
            continue

        function = item.get("function") or {}
        if not isinstance(function, dict):
            continue

        name = function.get("name")
        if not name:
            # A streamed native call may omit name in later fragments. Those
            # fragments are handled by normalize_stream_tool_deltas instead.
            continue

        arguments = function.get("arguments", "{}")
        if isinstance(arguments, (dict, list)):
            arguments = json.dumps(arguments, ensure_ascii=False)
        elif arguments is None:
            arguments = "{}"
        else:
            arguments = str(arguments)

        normalized.append(
            {
                "id": str(item.get("id") or _new_tool_call_id(index)),
                "type": "function",
                "function": {
                    "name": str(name),
                    "arguments": arguments,
                },
            }
        )
    return normalized


def normalize_stream_tool_deltas(tool_calls: Any) -> List[Dict[str, Any]]:
    """Preserve OpenAI incremental tool-call fragments in SSE mode.

    Native APIs commonly stream a name/id in the first delta and only an
    arguments fragment in later deltas. Hermes needs those fragments unchanged,
    so this function deliberately does not require `function.name` on every
    chunk.
    """

    if not isinstance(tool_calls, list):
        return []

    normalized: List[Dict[str, Any]] = []
    for fallback_index, item in enumerate(tool_calls):
        if not isinstance(item, dict):
            continue

        out: Dict[str, Any] = {
            "index": item.get("index", fallback_index),
        }
        if item.get("id") is not None:
            out["id"] = str(item["id"])
        if item.get("type") is not None:
            out["type"] = str(item["type"])

        function = item.get("function")
        if isinstance(function, dict):
            function_out: Dict[str, Any] = {}
            if function.get("name") is not None:
                function_out["name"] = str(function["name"])
            if function.get("arguments") is not None:
                arguments = function["arguments"]
                if isinstance(arguments, (dict, list)):
                    arguments = json.dumps(arguments, ensure_ascii=False)
                function_out["arguments"] = str(arguments)
            if function_out:
                out["function"] = function_out

        # Keep only meaningful deltas. `index` alone is not actionable.
        if len(out) > 1:
            normalized.append(out)

    return normalized


def parse_dsml_tool_calls(content: str) -> Tuple[str, List[Dict[str, Any]]]:
    """Extract DSML calls and remove the complete DSML blocks from content."""

    if not content or _DSML_TOOL_START not in content:
        return content or "", []

    tool_calls: List[Dict[str, Any]] = []

    for block_match in _DSML_TOOL_BLOCK_RE.finditer(content):
        block = block_match.group(1)
        for invoke_match in _DSML_INVOKE_RE.finditer(block):
            function_name = html.unescape(invoke_match.group(1)).strip()
            invoke_body = invoke_match.group(2)
            if not function_name:
                continue

            arguments: Dict[str, Any] = {}
            for parameter_match in _DSML_PARAM_RE.finditer(invoke_body):
                parameter_name = html.unescape(parameter_match.group(1)).strip()
                if not parameter_name:
                    continue

                is_string = parameter_match.group(2).lower() == "true"
                arguments[parameter_name] = _parse_parameter_value(
                    parameter_match.group(3),
                    declared_string=is_string,
                )

            tool_calls.append(
                {
                    "id": _new_tool_call_id(len(tool_calls)),
                    "type": "function",
                    "function": {
                        "name": function_name,
                        "arguments": json.dumps(arguments, ensure_ascii=False),
                    },
                }
            )

    cleaned = _DSML_TOOL_BLOCK_RE.sub("", content)
    # Remove standalone DSML tags too. If an incomplete DSML block remains,
    # drop its tail: it represents an invocation, never user-facing prose.
    if _DSML_OPEN in cleaned:
        cleaned = cleaned.split(_DSML_OPEN, 1)[0]
    cleaned = _DSML_ANY_TAG_RE.sub("", cleaned)
    return cleaned, tool_calls


def clean_visible_text(content: str) -> str:
    """Apply only safe cleanup; do not invent word boundaries in tool input."""

    if not content:
        return ""

    content = content.replace("\xa0", " ")
    content = re.sub(r"\n{3,}", "\n\n", content)
    content = re.sub(r"[ \t]{2,}", " ", content)
    return content.strip()


def extract_response_content(
    result: Dict[str, Any],
) -> Tuple[str, List[Dict[str, Any]], str]:
    """Read native OpenAI or DSML-shaped non-streaming upstream output."""

    choices = result.get("choices")
    if not isinstance(choices, list) or not choices:
        raise UpstreamEmptyResponse("Empty response from upstream")

    choice = choices[0] if isinstance(choices[0], dict) else {}
    message = choice.get("message") or {}
    if not isinstance(message, dict):
        raise UpstreamEmptyResponse("Invalid message in upstream response")

    # DeepSeek reasoning models put the actual output in `reasoning_content`
    # and return `content: null`. Fall back to reasoning_content when content
    # is empty so we don't raise a false "Empty Response" 502.
    content = (
        message.get("content")
        or message.get("reasoning_content")
        or message.get("reasoning")
        or ""
    )
    if not isinstance(content, str):
        content = str(content)

    native_calls = normalize_tool_calls(message.get("tool_calls"))
    cleaned_content, dsml_calls = parse_dsml_tool_calls(content)
    tool_calls = native_calls or dsml_calls
    cleaned_content = clean_visible_text(cleaned_content)

    if not cleaned_content and not tool_calls:
        raise UpstreamEmptyResponse("No visible content or tool calls in upstream response")

    finish_reason = "tool_calls" if tool_calls else (choice.get("finish_reason") or "stop")

    return cleaned_content, tool_calls, str(finish_reason)


def _trailing_partial(text: str, marker: str) -> int:
    """Length of the suffix of text that may be a prefix of marker."""

    max_length = min(len(text), len(marker))
    for length in range(max_length, 0, -1):
        if marker.startswith(text[-length:]):
            return length
    return 0


class DSMLStreamParser:
    """Stateful parser that emits text and OpenAI tool calls from DSML streams.

    It never forwards bytes inside a DSML `tool_calls` block as visible text.
    """

    def __init__(self) -> None:
        self._buffer = ""
        self._in_tool_block = False
        self._block = ""

    def feed(self, chunk: str) -> Tuple[str, List[Dict[str, Any]]]:
        if not chunk:
            return "", []

        self._buffer += chunk
        visible_parts: List[str] = []
        tool_calls: List[Dict[str, Any]] = []

        while self._buffer:
            if not self._in_tool_block:
                start_index = self._buffer.find(_DSML_TOOL_START)
                if start_index == -1:
                    partial = _trailing_partial(self._buffer, _DSML_TOOL_START)
                    safe_length = len(self._buffer) - partial
                    if safe_length:
                        visible_parts.append(self._buffer[:safe_length])
                    self._buffer = self._buffer[safe_length:] if partial else ""
                    break

                if start_index:
                    visible_parts.append(self._buffer[:start_index])

                self._buffer = self._buffer[start_index + len(_DSML_TOOL_START):]
                self._block = _DSML_TOOL_START
                self._in_tool_block = True
                continue

            # We are inside a complete or partial DSML tool_calls block.
            end_index = self._buffer.find(_DSML_TOOL_END)
            if end_index == -1:
                partial = _trailing_partial(self._buffer, _DSML_TOOL_END)
                safe_length = len(self._buffer) - partial
                if safe_length:
                    self._block += self._buffer[:safe_length]
                self._buffer = self._buffer[safe_length:] if partial else ""
                break

            self._block += self._buffer[:end_index] + _DSML_TOOL_END
            self._buffer = self._buffer[end_index + len(_DSML_TOOL_END):]
            _, parsed_calls = parse_dsml_tool_calls(self._block)
            tool_calls.extend(parsed_calls)
            self._block = ""
            self._in_tool_block = False

        return "".join(visible_parts), tool_calls

    def flush(self) -> str:
        # An incomplete DSML block is intentionally discarded. It is never a
        # safe user-visible answer or an executable, complete tool call.
        if self._in_tool_block:
            self._buffer = ""
            self._block = ""
            return ""

        remaining = self._buffer
        self._buffer = ""
        return remaining

"""Validasi routing endpoint dinamis + reverse bridge (harus ALL PASSED).

Jalankan dari repo root:  python test/test_reverse_bridge.py
Keluar dengan kode 0 bila semua passed, 1 bila ada yang gagal.

Latar: model chat/messages-native (mimo, deepseek, ...) dijawab 500 oleh
upstream bila dipaksa lewat Responses API (live 2026-09-23). Gateway kini
me-resolve kategori native tiap model (model_endpoints) dan menjembatani
balik request Responses -> pipeline chat (chat_bridge).

Cakupan (semua offline, tanpa network):
  A. get_model_endpoint: kategori benar per keluarga + fallback + override
  B. build_chat_request_from_responses: input/tools/limit mapping
  C. chat_completion_to_responses: bentuk objek Responses
  D. chat_stream_to_responses_stream: event SSE Responses
"""
import asyncio
import json
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS, FAIL = "PASS", "FAIL"
_results = []


def case(name):
    def deco(fn):
        _results.append((name, fn))
        return fn
    return deco


# ---------- A. kategori endpoint ----------

@case("A1 responses-native: spark/gpt/grok")
def _a1():
    from app.services.model_endpoints import get_model_endpoint
    for m in ("muse-spark-1.3-contributor-free", "muse-spark-1.2",
              "gpt-5", "gpt-5.3-codex-spark", "grok-4.5", "grok-build-0.1"):
        assert get_model_endpoint(m) == "responses", m


@case("A2 chat-native: mimo/deepseek/glm/kimi/minimax/ling/nemotron/pickle")
def _a2():
    from app.services.model_endpoints import get_model_endpoint
    for m in ("mimo-v2.6-flash-free", "mimo-v2.5-free",
              "deepseek-v4-flash-free", "glm-5.2", "kimi-k2.5",
              "minimax-m3", "ling-3.0-flash-fin-free",
              "nemotron-3-ultra-free", "big-pickle"):
        assert get_model_endpoint(m) == "chat", m


@case("A3 messages-native: claude/qwen")
def _a3():
    from app.services.model_endpoints import get_model_endpoint
    for m in ("claude-sonnet-4-5", "claude-opus-5", "claude-haiku-4-5",
              "qwen3.5-plus", "qwen3.8-flash"):
        assert get_model_endpoint(m) == "messages", m


@case("A4 fallback: unknown -> chat; case-insensitive; kosong -> chat")
def _a4():
    from app.services.model_endpoints import get_model_endpoint
    assert get_model_endpoint("model-baru-xyz") == "chat"
    assert get_model_endpoint("MIMO-V2.6-FLASH-FREE") == "chat"
    assert get_model_endpoint("") == "chat"
    assert get_model_endpoint(None) == "chat"
    # Prefix future-proof: spark/gpt generasi baru ikut keluarga.
    assert get_model_endpoint("muse-spark-9.9") == "responses"
    assert get_model_endpoint("gpt-9.9-turbo") == "responses"


@case("A5 env override exact + prefix* dihormati")
def _a5():
    from app.services import model_endpoints as me
    old = os.getenv("MODEL_ENDPOINT_OVERRIDES_JSON")
    os.environ["MODEL_ENDPOINT_OVERRIDES_JSON"] = json.dumps(
        {"mimo-v2.6-flash-free": "responses", "qwen*": "chat"})
    try:
        assert me.get_model_endpoint("mimo-v2.6-flash-free") == "responses"
        assert me.get_model_endpoint("qwen3.5-plus") == "chat"
        assert me.get_model_endpoint("gpt-5") == "responses"  # tak tersentuh
    finally:
        if old is None:
            os.environ.pop("MODEL_ENDPOINT_OVERRIDES_JSON", None)
        else:
            os.environ["MODEL_ENDPOINT_OVERRIDES_JSON"] = old


@case("A6 is_responses_native konsisten dengan tabel")
def _a6():
    from app.services.model_endpoints import is_responses_native
    assert is_responses_native("muse-spark-1.3-contributor-free") is True
    assert is_responses_native("mimo-v2.6-flash-free") is False
    assert is_responses_native("claude-sonnet-4-5") is False
    assert is_responses_native("") is False


# ---------- B. Responses -> chat request ----------

@case("B1 input string + max_output_tokens -> messages + max_tokens")
def _b1():
    from app.services.chat_bridge import build_chat_request_from_responses
    req = build_chat_request_from_responses(
        {"model": "mimo-v2.6-flash-free", "input": "jawab: pong",
         "max_output_tokens": 64, "store": False}, "mimo-v2.6-flash-free")
    assert req.model == "mimo-v2.6-flash-free"
    assert len(req.messages) == 1 and req.messages[0].role == "user"
    assert req.messages[0].content == "jawab: pong"
    assert req.max_tokens == 64


@case("B2 instructions/system -> system message; input list multi-role")
def _b2():
    from app.services.chat_bridge import build_chat_request_from_responses
    req = build_chat_request_from_responses(
        {"model": "mimo-v2.5-free",
         "instructions": "You are helpful.",
         "input": [
             {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
             {"type": "function_call_output", "call_id": "c1", "output": "ok"},
         ]}, "mimo-v2.5-free")
    roles = [m.role for m in req.messages]
    assert roles[0] == "system" and req.messages[0].content == "You are helpful."
    assert "user" in roles and "tool" in roles, roles


@case("B3 tools flat -> chat function tools; reasoning effort diteruskan")
def _b3():
    from app.services.chat_bridge import build_chat_request_from_responses
    req = build_chat_request_from_responses(
        {"model": "deepseek-v4-flash-free", "input": "t",
         "tools": [{"type": "function", "name": "bash",
                    "description": "run", "parameters": {"type": "object", "properties": {}}}],
         "tool_choice": "auto",
         "reasoning": {"effort": "high"}}, "deepseek-v4-flash-free")
    assert req.tools and req.tools[0]["function"]["name"] == "bash"
    assert req.tool_choice == "auto"
    assert req.reasoning_effort == "high"


@case("B4 input kosong -> fallback Hello (kontrak messages non-empty)")
def _b4():
    from app.services.chat_bridge import build_chat_request_from_responses
    req = build_chat_request_from_responses(
        {"model": "mimo-v2.5-free", "input": []}, "mimo-v2.5-free")
    assert len(req.messages) == 1


# ---------- C. chat completion -> Responses object ----------

@case("C1 teks + usage terpetakan ke bentuk Responses")
def _c1():
    from app.services.chat_bridge import chat_completion_to_responses
    result = {
        "id": "chatcmpl-abc", "created": 123,
        "choices": [{"message": {"role": "assistant", "content": "pong"},
                     "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
    }
    obj = chat_completion_to_responses(result, "mimo-v2.5-free")
    assert obj["object"] == "response" and obj["model"] == "mimo-v2.5-free"
    assert obj["status"] == "completed"
    assert obj["output"][0]["content"][0] == {"type": "output_text", "text": "pong"}
    assert obj["usage"] == {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12}


@case("C2 tool_calls -> function_call items")
def _c2():
    from app.services.chat_bridge import chat_completion_to_responses
    result = {
        "choices": [{"message": {
            "role": "assistant", "content": "",
            "tool_calls": [{"id": "call_1", "type": "function",
                            "function": {"name": "bash", "arguments": "{}"}}]},
            "finish_reason": "tool_calls"}],
        "usage": {},
    }
    obj = chat_completion_to_responses(result, "glm-5")
    fc = [i for i in obj["output"] if i.get("type") == "function_call"]
    assert len(fc) == 1 and fc[0]["name"] == "bash" and fc[0]["call_id"] == "call_1"


# ---------- D. chat SSE -> Responses SSE ----------

def _run(coro):
    return asyncio.run(coro)


async def _collect(gen):
    out = []
    async for raw in gen:
        out.append(raw)
    return out


@case("D1 delta teks mengalir live + completed + DONE")
def _d1():
    from app.services.chat_bridge import chat_stream_to_responses_stream

    async def fake_chat():
        yield 'data: {"choices":[{"delta":{"role":"assistant"},"finish_reason":null}]}\n\n'
        yield 'data: {"choices":[{"delta":{"content":"po"},"finish_reason":null}]}\n\n'
        yield 'data: {"choices":[{"delta":{"content":"ng"},"finish_reason":null}]}\n\n'
        yield 'data: {"choices":[{"delta":{},"finish_reason":"stop"}], "usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}\n\n'
        yield "data: [DONE]\n\n"

    frames = _run(_collect(chat_stream_to_responses_stream(
        fake_chat(), client_model="mimo-v2.5-free")))
    joined = "\n".join(frames)
    assert "response.created" in joined
    assert joined.count("response.output_text.delta") == 2, joined
    assert "response.completed" in joined
    assert frames[-1] == "data: [DONE]\n\n"
    completed = json.loads(
        [f for f in frames if "response.completed" in f][0][len("data: "):])
    text = completed["response"]["output"][0]["content"][0]["text"]
    assert text == "pong", text
    assert completed["response"]["usage"]["total_tokens"] == 6


@case("D2 error chunk tanpa konten -> error + DONE (konvensi sama)")
def _d2():
    from app.services.chat_bridge import chat_stream_to_responses_stream

    async def fake_err():
        yield 'data: {"error": {"message": "boom", "code": "X"}}\n\n'
        yield "data: [DONE]\n\n"

    frames = _run(_collect(chat_stream_to_responses_stream(
        fake_err(), client_model="glm-5")))
    assert any('"error"' in f for f in frames), frames
    assert frames[-1] == "data: [DONE]\n\n"


@case("D3 keepalive ':' diteruskan apa adanya")
def _d3():
    from app.services.chat_bridge import chat_stream_to_responses_stream

    async def fake_keep():
        yield ":\n\n"
        yield 'data: {"choices":[{"delta":{"content":"x"},"finish_reason":null}]}\n\n'
        yield "data: [DONE]\n\n"

    frames = _run(_collect(chat_stream_to_responses_stream(
        fake_keep(), client_model="kimi-k2.5")))
    assert frames[0] == ":\n\n", frames
    assert any("response.output_text.delta" in f for f in frames)


def main() -> int:
    print(f"menjalankan {len(_results)} case...")
    print("=" * 70)
    passed = failed = 0
    for name, fn in _results:
        try:
            fn()
        except Exception:
            failed += 1
            print(f"[{FAIL}] {name}")
            traceback.print_exc(limit=3)
        else:
            passed += 1
            print(f"[{PASS}] {name}")
    print("=" * 70)
    print(f"hasil: {passed} passed, {failed} failed dari {len(_results)} case")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

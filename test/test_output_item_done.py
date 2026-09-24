"""Validasi penanganan output_item.done + akuntansi tipe event (harus ALL PASSED).

Jalankan dari repo root:  python test/test_output_item_done.py
Keluar dengan kode 0 bila semua passed, 1 bila ada yang gagal.

Latar (live 2026-09-24): stream spark 302 dtk, wire 88KB dalam 3 event,
reasoning_events=0, tanpa konten -> EMPTY_RESPONSE. Diduga kuat komposisi
event: created + output_item.done (reasoning xhigh utuh, tanpa summary
delta sebelumnya) + completed. Cabang output_item.done belum ada sehingga
thinking puluhan KB di-drop diam-diam.

Cakupan (HTTP dimock, tanpa network):
  A. output_item.done reasoning -> reasoning_content live + anti-empty
  B. output_item.done message (tanpa delta) -> content (tidak EMPTY)
  C. delta reasoning lalu done sama -> TIDAK ganda di layar
  D. log SUMMARY/EMPTY memuat rincian types=
  E. unit _format_event_types
"""
import asyncio
import json
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

PASS, FAIL = "PASS", "FAIL"
_results = []


def case(name):
    def deco(fn):
        _results.append((name, fn))
        return fn
    return deco


class _FakeResponse:
    def __init__(self, status_code: int, body: bytes, content_type: str = "text/event-stream"):
        self.status_code = status_code
        self.headers = httpx.Headers({"content-type": content_type})
        self._body = body
        self.text = body.decode("utf-8", "replace")

    async def aread(self):
        return self._body

    def aiter_lines(self):
        return self._aiter_lines()

    async def _aiter_lines(self):
        for line in self.text.split("\n"):
            yield line


class _FakeStreamCtx:
    def __init__(self, response: _FakeResponse):
        self._response = response

    async def __aenter__(self):
        return self._response

    async def __aexit__(self, *exc):
        return False


class _ScriptClient:
    """Skenario SSE tetap per attempt ( Tantangan: tiru insiden 302 dtk )."""

    def __init__(self, body: bytes):
        self.calls: list = []
        self._body = body

    def stream(self, method, url, json=None, headers=None, timeout=None):
        self.calls.append({"url": url, "payload": json, "headers": headers})
        return _FakeStreamCtx(_FakeResponse(200, self._body))


def _install_fake(fake: _ScriptClient):
    import app.services.responses_bridge as rb
    rb._get_http = lambda: fake


def _drive(payload_extra_body: bytes, model="muse-spark-1.3-contributor-free"):
    import app.services.responses_bridge as rb
    from fastapi import BackgroundTasks

    async def run():
        gen = rb.responses_to_chat_stream_generator(
            {"model": model, "messages": [{"role": "user", "content": "hi"}],
             "stream": True},
            client_model=model,
            include_usage_requested=True,
            background_tasks=BackgroundTasks(),
            use_relay=False,
            opencode_headers={"x-opencode-session": "ses_TESTITEMDONE"},
        )
        return [c async for c in gen]

    return asyncio.run(run())


def _sse_created(rid="r1"):
    return f'data: {{"type":"response.created","response":{{"id":"{rid}"}}}}\n\n'.encode()


def _sse_done_reasoning(trace="THINKING-TRACE-ABC"):
    item = {"id": "rs1", "type": "reasoning",
            "summary": [{"type": "summary_text", "text": trace}]}
    return f'data: {{"type":"response.output_item.done","output_index":0,"item":{json.dumps(item)}}}\n\n'.encode()


def _sse_done_message(text="HELLO-DONE"):
    item = {"id": "m1", "type": "message", "role": "assistant",
            "content": [{"type": "output_text", "text": text}]}
    return f'data: {{"type":"response.output_item.done","output_index":0,"item":{json.dumps(item)}}}\n\n'.encode()


def _sse_completed(rid="r1"):
    return (f'data: {{"type":"response.completed","response":{{"id":"{rid}",'
            f'"usage":{{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}}}}\n\n'
            ).encode()


def _sse_delta_reasoning(text="PART1"):
    return (f'data: {{"type":"response.reasoning_summary_text.delta",'
            f'"item_id":"rs1","delta":{json.dumps(text)}}}\n\n').encode()


# ---------- A/B/C: perilaku bridge ----------

@case("A1 done reasoning -> reasoning_content live, bukan EMPTY")
def _a1():
    body = (_sse_created() + _sse_done_reasoning() + _sse_completed()
            + b"data: [DONE]\n\n")
    _install_fake(_ScriptClient(body))
    chunks = _drive(body)
    raw = "".join(chunks)
    assert "THINKING-TRACE-ABC" in raw, raw[-800:]
    assert "EMPTY_RESPONSE" not in raw, raw[-800:]
    assert chunks[-1] == "data: [DONE]\n\n"


@case("B1 done message tanpa delta -> content, bukan EMPTY")
def _b1():
    body = (_sse_created() + _sse_done_message() + _sse_completed()
            + b"data: [DONE]\n\n")
    _install_fake(_ScriptClient(body))
    chunks = _drive(body)
    raw = "".join(chunks)
    assert "HELLO-DONE" in raw, raw[-800:]
    assert "EMPTY_RESPONSE" not in raw, raw[-800:]
    assert chunks[-1] == "data: [DONE]\n\n"


@case("C1 delta lalu done sama -> reasoning tidak ganda di layar")
def _c1():
    body = (_sse_created() + _sse_delta_reasoning("PART1")
            + _sse_done_reasoning("PART1-FULL") + _sse_completed()
            + b"data: [DONE]\n\n")
    _install_fake(_ScriptClient(body))
    chunks = _drive(body)
    raw = "".join(chunks)
    assert raw.count('"reasoning_content"') == 1, raw[-800:]
    assert "PART1" in raw and "EMPTY_RESPONSE" not in raw


# ---------- D: akuntansi tipe event di log ----------

@case("D1 SUMMARY + EMPTY memuat rincian types=")
def _d1():
    import app.services.responses_bridge as rb
    logs: list = []
    orig = rb._log
    rb._log = lambda tag, msg: logs.append(f"{tag} {msg}")
    try:
        # Stream kosong total (created + completed saja) -> jalur EMPTY.
        body = (_sse_created() + _sse_completed() + b"data: [DONE]\n\n")
        _install_fake(_ScriptClient(body))
        chunks = _drive(body)
    finally:
        rb._log = orig
    raw = "".join(chunks)
    assert "EMPTY_RESPONSE" in raw  # kontrol: memang jalur empty
    joined = "\n".join(logs)
    assert "types=response.completed:1" in joined, joined[-600:]
    assert "response.created:1" in joined, joined[-600:]


# ---------- E: unit formatter ----------

@case("E1 _format_event_types: kosong/normal/aneh")
def _e1():
    from app.services.responses_bridge import _format_event_types
    assert _format_event_types({}) == "-"
    assert _format_event_types(None) == "-"
    assert _format_event_types({"b": 2, "a": 1}) == "a:1,b:2"
    assert _format_event_types("bukan-dict") == "-"


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
            traceback.print_exc(limit=5)
        else:
            passed += 1
            print(f"[{PASS}] {name}")
    print("=" * 70)
    print(f"hasil: {passed} passed, {failed} failed dari {len(_results)} case")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

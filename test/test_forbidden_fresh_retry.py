"""Validasi fresh-session retry untuk 403 FreeTierError (harus ALL PASSED).

Jalankan dari repo root:  python test/test_forbidden_fresh_retry.py
Keluar dengan kode 0 bila semua passed, 1 bila ada yang gagal.

Latar (live 2026-09-23): request mimo via reverse bridge kena 403 di
relay-04, relay-05, DAN direct dengan fingerprint identik yang lolos 5
detik sebelumnya. 403 di direct = yang di-flag BUKAN IP relay melainkan
identitas request (sesi di-flag / detector transien) — rotasi IP tidak
akan sembuh. Perbaikan: bila SEMUA target 403 pra-payload (nol konten
terkirim), SATU percobaan terakhir ke target terakhir dengan session +
request ID baru, lalu alur normal dilanjutkan.

Cakupan (semua offline, HTTP dimock):
  A. Knob config ada + default waras
  B. Helper identitas fresh (format valid, beda, preservasi header)
  C. stream_generator: all-403 -> fresh retry -> sukses (sesi baru)
  D. stream_generator: 403 lalu 429 -> TANPA fresh retry (RATE_LIMITED)
  E. responses_stream_generator: all-403 + replay reasoning -> TANPA retry
  F. responses_stream_generator: all-403 bersih -> fresh retry -> sukses
  G. responses_to_chat_stream_generator memuat blok fase yang sama
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import inspect
import traceback

from fastapi import BackgroundTasks

PASS, FAIL = "PASS", "FAIL"
_results = []


def case(name):
    def deco(fn):
        _results.append((name, fn))
        return fn
    return deco


FORBIDDEN_BODY = (
    b'{"type":"error","error":{"type":"FreeTierError",'
    b'"message":"free tier can only be used in OpenCode"}}'
)


class _FakeResp:
    def __init__(self, status, lines=None, body=b""):
        self.status_code = status
        self._lines = list(lines or [])
        self._body = body
        self.headers = {}

    @property
    def text(self):
        return self._body.decode("utf-8", errors="replace")

    async def aread(self):
        return self._body

    async def aiter_lines(self):
        for line in self._lines:
            yield line


class _FakeCM:
    def __init__(self, resp):
        self._resp = resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, *args):
        return False


class _FakeClient:
    """Klien HTTP tiruan: script = list [(status, lines|body), ...]."""

    def __init__(self, script):
        self._script = list(script)
        self.calls = []

    def stream(self, method, url, json=None, headers=None, **kwargs):
        self.calls.append({"url": url, "headers": dict(headers or {})})
        status, data = self._script.pop(0)
        if status == 200:
            return _FakeCM(_FakeResp(200, lines=data))
        return _FakeCM(_FakeResp(status, body=data))


def _chat_ok_lines(text="hi"):
    base = '{"id":"x","object":"chat.completion.chunk","created":1,"model":"m"'
    return [
        f"data: {base}" + ',"choices":[{"index":0,"delta":{"role":"assistant"}}]}',
        f"data: {base}" + f',"choices":[{{"index":0,"delta":{{"content":"{text}"}}}}]}}',
        f"data: {base}" + ',"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}',
        "data: [DONE]",
    ]


def _resp_ok_lines():
    return [
        'data: {"type":"response.created","response":{"id":"resp-1"}}',
        'data: {"type":"response.output_text.delta","delta":"hi"}',
        "data: [DONE]",
    ]


def _base_headers(session="ses_abcdef1234567890abcdefghij"):
    return {
        "x-opencode-client": "cli",
        "x-opencode-project": "global",
        "x-opencode-session": session,
        "x-opencode-request": "msg_abcdef1234567890abcdefghij",
        "User-Agent": "opencode/1.18.31 test",
    }


# ---------- A. config ----------

@case("A1 knob fresh-session retry ada + default waras")
def _a1():
    import app.core.config as c
    assert c.FORBIDDEN_FRESH_SESSION_RETRY is True
    assert isinstance(c.FORBIDDEN_RETRY_DELAY, float) and c.FORBIDDEN_RETRY_DELAY >= 0


# ---------- B. helper ----------

@case("B1 identitas fresh: format valid, beda, preservasi header")
def _b1():
    from app.services.opencode import (
        _fresh_identity_headers,
        _is_valid_opencode_id,
    )
    base = _base_headers()
    fresh = _fresh_identity_headers(base)
    assert _is_valid_opencode_id(fresh["x-opencode-session"], "ses_")
    assert _is_valid_opencode_id(fresh["x-opencode-request"], "msg_")
    assert fresh["x-opencode-session"] != base["x-opencode-session"]
    assert fresh["x-opencode-request"] != base["x-opencode-request"]
    assert fresh["x-opencode-client"] == "cli"
    assert fresh["x-opencode-project"] == "global"
    assert "opencode/1.18.31" in fresh["User-Agent"]
    # Base tidak dimutasi.
    assert base["x-opencode-session"].startswith("ses_abcdef")


# ---------- C/D. stream_generator ----------

def _run_stream(mod, gen):
    async def _collect():
        return [chunk async for chunk in gen]
    return asyncio.run(_collect())


@case("C1 chat all-403 -> fresh retry sesi baru -> sukses")
def _c1():
    import app.services.streaming as s
    old_delay, s.FORBIDDEN_RETRY_DELAY = s.FORBIDDEN_RETRY_DELAY, 0.0
    old_http, fake = s._get_http, _FakeClient([
        (403, FORBIDDEN_BODY),
        (200, _chat_ok_lines()),
    ])
    s._get_http = lambda: fake
    try:
        chunks = _run_stream(s, s.stream_generator(
            {"model": "mimo-v2.6-flash-free",
             "messages": [{"role": "user", "content": "hi"}],
             "stream": True},
            client_model="mimo-v2.6-flash-free",
            include_usage_requested=True,
            background_tasks=BackgroundTasks(),
            use_relay=False,
            opencode_headers=_base_headers(),
        ))
    finally:
        s._get_http = old_http
        s.FORBIDDEN_RETRY_DELAY = old_delay
    assert len(fake.calls) == 2, f"harus 1 direct + 1 fresh, got {len(fake.calls)}"
    sessions = [c["headers"]["x-opencode-session"] for c in fake.calls]
    assert sessions[0] == "ses_abcdef1234567890abcdefghij"
    assert sessions[1] != sessions[0], "attempt fresh wajib sesi baru"
    joined = "\n".join(chunks)
    assert '"content": "hi"' in joined or '"content":"hi"' in joined, joined[-500:]
    assert chunks[-1] == "data: [DONE]\n\n"


@case("D1 chat 403 lalu 429 -> TANPA fresh retry (RATE_LIMITED)")
def _d1():
    import app.services.streaming as s
    old_http, fake = s._get_http, _FakeClient([
        (403, FORBIDDEN_BODY),
        (429, b'{"error":"slow down"}'),
    ])
    old_batch = s._relay_batch_for_request
    old_limit = s._limit_stream_targets
    s._get_http = lambda: fake
    s._relay_batch_for_request = lambda for_stream=True: ["https://relay-test-1/"]
    s._limit_stream_targets = lambda targets: targets
    try:
        chunks = _run_stream(s, s.stream_generator(
            {"model": "mimo-v2.6-flash-free",
             "messages": [{"role": "user", "content": "hi"}],
             "stream": True},
            client_model="mimo-v2.6-flash-free",
            include_usage_requested=True,
            background_tasks=BackgroundTasks(),
            use_relay=True,
            opencode_headers=_base_headers(),
        ))
    finally:
        s._get_http = old_http
        s._relay_batch_for_request = old_batch
        s._limit_stream_targets = old_limit
    assert len(fake.calls) == 2, f"tanpa retry ekstra, got {len(fake.calls)}"
    joined = "\n".join(chunks)
    assert "RATE_LIMITED" in joined, joined[-500:]


# ---------- E/F. responses_stream_generator ----------

@case("E1 responses all-403 + replay reasoning -> TANPA fresh retry")
def _e1():
    import app.services.responses_bridge as b
    old_http, fake = b._get_http, _FakeClient([
        (403, FORBIDDEN_BODY),
        (403, FORBIDDEN_BODY),
    ])
    b._get_http = lambda: fake
    payload = {
        "model": "muse-spark-1.3-contributor-free",
        "input": [
            {"type": "reasoning", "encrypted_content": "secret-turn-1",
             "summary": []},
            {"role": "user", "content": "lanjutkan"},
        ],
        "stream": True,
    }
    try:
        chunks = _run_stream(b, b.responses_stream_generator(
            payload,
            client_model="muse-spark-1.3-contributor-free",
            background_tasks=BackgroundTasks(),
            use_relay=False,
            opencode_headers=_base_headers(),
        ))
    finally:
        b._get_http = old_http
    # Guard replay: setelah 403 pertama langsung all-failed TANPA percobaan
    # sesi baru (1 call) — identitas wajib stabil untuk issuance berikutnya.
    assert len(fake.calls) == 1, f"identitas wajib stabil, got {len(fake.calls)}"
    joined = "\n".join(chunks)
    assert "All responses targets failed" in joined, joined[-500:]


@case("F1 responses all-403 bersih -> fresh retry -> sukses")
def _f1():
    import app.services.responses_bridge as b
    old_delay, b.FORBIDDEN_RETRY_DELAY = b.FORBIDDEN_RETRY_DELAY, 0.0
    old_http, fake = b._get_http, _FakeClient([
        (403, FORBIDDEN_BODY),
        (200, _resp_ok_lines()),
    ])
    b._get_http = lambda: fake
    try:
        chunks = _run_stream(b, b.responses_stream_generator(
            {"model": "mimo-v2.6-flash-free", "input": "hi", "stream": True},
            client_model="mimo-v2.6-flash-free",
            background_tasks=BackgroundTasks(),
            use_relay=False,
            opencode_headers=_base_headers(),
        ))
    finally:
        b._get_http = old_http
        b.FORBIDDEN_RETRY_DELAY = old_delay
    assert len(fake.calls) == 2, f"harus 1 direct + 1 fresh, got {len(fake.calls)}"
    sessions = [c["headers"]["x-opencode-session"] for c in fake.calls]
    assert sessions[1] != sessions[0], "attempt fresh wajib sesi baru"
    joined = "\n".join(chunks)
    assert "response.output_text.delta" in joined, joined[-500:]
    assert chunks[-1] == "data: [DONE]\n\n"


# ---------- G. struktural generator-3 ----------

@case("G1 bridge chat generator memuat blok fase + guard replay")
def _g1():
    import app.services.responses_bridge as b
    import app.services.streaming as s
    src = inspect.getsource(b.responses_to_chat_stream_generator)
    assert "FRESH-SESSION-RETRY" in src
    assert "_payload_has_replay_reasoning(payload)" in src
    assert "forbidden_count" in src
    # Kedua generator responses + generator chat memakai mekanisme sama.
    assert inspect.getsource(b).count("FRESH-SESSION-RETRY") == 2
    assert "FRESH-SESSION-RETRY" in inspect.getsource(s.stream_generator)


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

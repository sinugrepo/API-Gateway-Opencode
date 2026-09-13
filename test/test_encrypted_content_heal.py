"""Validasi auto-heal penolakan replay reasoning.encrypted_content.

Jalankan dari repo root:  python test/test_encrypted_content_heal.py
Keluar 0 bila semua passed, 1 bila ada yang gagal.

Latar: KiloCode/stateless Responses mereplay item reasoning ber-
`encrypted_content` dari turn sebelumnya. Konten itu di-issuance ke caller
identity turn pertama; bila tidak cocok lagi, upstream menolak SELURUH
request dengan 400 "reasoning `encrypted_content` was not issued to this
caller" — di SEMUA target (relay maupun direct). Auto-heal: buang item
reasoning replay dari input lalu ulangi dari target pertama (sekali).

Cakupan:
  A. Detektor pesan error + detektor payload
  B. _strip_replayed_reasoning (buang reasoning replay, sisakan lainnya)
  C. Header CLI: format sesi ala CLI asli + User-Agent (regresi)
  D. He2l end-to-end generator: HTTP 400 langsung -> heal -> sukses
  E. Heal via relay.error early-SSE (HTTP 200 + event relay.error)
  F. Tidak heal untuk error lain / hanya sekali / tanpa reasoning replay
"""
import asyncio
import json
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

PASS, FAIL = "PASS", "FAIL"
_results = []


def case(name):
    def deco(fn):
        _results.append((name, fn))
        return fn
    return deco


_ENC_MSG = (
    '{"model":"muse-spark-1.3-contributor-free","error":{"param":null,'
    '"type":"invalid_request_error","message":"Error from provider (Console): '
    'Upstream request failed: [invalid_request_error] reasoning '
    '`encrypted_content` was not issued to this caller"}}'
)


def _replay_payload():
    return {
        "model": "muse-spark-1.3-contributor-free",
        "store": False,
        "include": ["reasoning.encrypted_content"],
        "input": [
            {"type": "reasoning", "summary": [],
             "encrypted_content": "gAAAAOldCallerIssuedBlob=="},
            {"type": "message", "role": "user",
             "content": [{"type": "input_text", "text": "turn 1"}]},
            {"type": "reasoning", "summary": [],
             "encrypted_content": "gAAAAOtherBlob=="},
            {"type": "message", "role": "user",
             "content": [{"type": "input_text", "text": "turn 2"}]},
        ],
    }


# ---------- A. detektor ----------

@case("A1 _is_encrypted_content_rejection: pesan persis -> True")
def _a1():
    from app.services.responses_bridge import _is_encrypted_content_rejection
    assert _is_encrypted_content_rejection(_ENC_MSG) is True
    assert _is_encrypted_content_rejection(
        "Upstream responded with 400: reasoning `encrypted_content` was not issued to this caller"
    ) is True


@case("A2 pesan lain (400 biasa, 429, kosong) -> False")
def _a2():
    from app.services.responses_bridge import _is_encrypted_content_rejection
    assert _is_encrypted_content_rejection("invalid model name") is False
    assert _is_encrypted_content_rejection(
        '{"error":{"message":"rate limit"}}') is False
    assert _is_encrypted_content_rejection("") is False
    # Menyebut encrypted_content tanpa frasa "not issued" TIDAK kena heal.
    assert _is_encrypted_content_rejection(
        "reasoning encrypted_content malformed") is False


@case("A3 _payload_has_replay_reasoning: hanya input reasoning ber-encrypted")
def _a3():
    from app.services.responses_bridge import _payload_has_replay_reasoning
    assert _payload_has_replay_reasoning(_replay_payload()) is True
    no_enc = _replay_payload()
    no_enc["input"] = [i for i in no_enc["input"] if i.get("type") != "reasoning"]
    assert _payload_has_replay_reasoning(no_enc) is False
    assert _payload_has_replay_reasoning({"input": "teks"}) is False
    assert _payload_has_replay_reasoning({}) is False
    assert _payload_has_replay_reasoning(None) is False


# ---------- B. strip ----------

@case("B1 _strip_replayed_reasoning buang semua item reasoning replay")
def _b1():
    from app.services.responses_bridge import _strip_replayed_reasoning
    payload = _replay_payload()
    healed, removed = _strip_replayed_reasoning(payload)
    assert removed == 2, removed
    assert all(item.get("type") != "reasoning" for item in healed["input"]), healed["input"]
    assert len(healed["input"]) == 2
    # Teks turn dipertahankan utuh.
    texts = [p["text"] for item in healed["input"] for p in item["content"]]
    assert texts == ["turn 1", "turn 2"], texts
    # Payload ASLI tidak dimutasi.
    assert len(payload["input"]) == 4
    assert payload["input"][0]["type"] == "reasoning"


@case("B2 payload tanpa reasoning replay -> tidak berubah (removed=0)")
def _b2():
    from app.services.responses_bridge import _strip_replayed_reasoning
    plain = {"model": "m", "input": [{"role": "user",
            "content": [{"type": "input_text", "text": "hai"}]}]}
    healed, removed = _strip_replayed_reasoning(plain)
    assert removed == 0
    assert healed is plain  # objek sama, bukan salinan
    healed2, removed2 = _strip_replayed_reasoning({"model": "m"})
    assert removed2 == 0 and healed2 == {"model": "m"}


# ---------- C. regresi identitas CLI ----------

@case("C1 sesi stabil format CLI (ses_ + 26 alfanumerik campur)")
def _c1():
    import string as _st
    from app.services.opencode import _stable_opencode_session
    s = _stable_opencode_session({"prompt_cache_key": "ref-test"})
    assert s.startswith("ses_") and len(s) == 30, s
    assert all(c in _st.ascii_letters + _st.digits for c in s[4:]), s
    assert s == _stable_opencode_session({"prompt_cache_key": "ref-test"})
    assert s != _stable_opencode_session({"prompt_cache_key": "lain"})


@case("C2 User-Agent CLI asli ada di header CLI")
def _c2():
    from app.services.opencode import _opencode_cli_headers
    ua = _opencode_cli_headers()["User-Agent"]
    assert ua.startswith("opencode/"), ua
    assert "ai-sdk/" in ua and "runtime/" in ua, ua


# ---------- D. heal end-to-end (HTTP 400 langsung) ----------

class _FakeAsyncBytes:
    def __init__(self, data: bytes):
        self._data = data

    async def aread(self):
        return self._data


class _FakeResponse:
    """Response httpx tiruan: status + body JSON/SSE, tanpa network."""

    def __init__(self, status_code: int, body: bytes):
        self.status_code = status_code
        self.headers = httpx.Headers({"content-type": "application/json"})
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


class _HealFakeClient:
    """Client tiruan: attempt 1..N jawab 400 encrypted, setelah heal -> 200 SSE."""

    def __init__(self, fail_with_sse_relay: bool = False):
        self.calls: list = []
        self._fail_with_sse_relay = fail_with_sse_relay

    def _mk_response(self, payload_seen: dict):
        healed = not any(
            isinstance(i, dict) and i.get("type") == "reasoning"
            for i in (payload_seen.get("input") or [])
        )
        if not healed:
            if self._fail_with_sse_relay:
                # Relay early-SSE: HTTP 200 + event relay.error berisi 400.
                event = json.dumps({"type": "relay.error", "status": 400,
                                    "body": _ENC_MSG})
                body = f": relay-connected\n\ndata: {event}\n\ndata: [DONE]\n\n".encode()
                return _FakeResponse(200, body)
            return _FakeResponse(400, _ENC_MSG.encode())
        # Setelah heal: 200 SSE minimal yang selesai.
        body = (
            b'data: {"type":"response.created","response":{"id":"r1"}}\n\n'
            b'data: {"type":"response.output_text.delta","item_id":"m1",'
            b'"delta":"healed!"}\n\n'
            b'data: {"type":"response.completed","response":{"id":"r1",'
            b'"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}\n\n'
            b"data: [DONE]\n\n"
        )
        return _FakeResponse(200, body)

    def stream(self, method, url, json=None, headers=None, timeout=None):
        self.calls.append({"url": url, "payload": json, "headers": headers})
        return _FakeStreamCtx(self._mk_response(json or {}))

    async def aiter_lines_wrapper(self):  # pragma: no cover - tak dipakai
        yield ""


def _install_fake_client(monkeypatch_target, fake: _HealFakeClient):
    from app.core import http_client
    http_client._get_http = lambda: fake
    import app.services.responses_bridge as rb
    rb._get_http = lambda: fake


@case("D1 pass-through: 400 encrypted -> heal -> 200 sukses (3 target cukup)")
def _d1():
    import app.services.responses_bridge as rb
    from fastapi import BackgroundTasks

    fake = _HealFakeClient()
    _install_fake_client(None, fake)

    async def run():
        gen = rb.responses_stream_generator(
            _replay_payload(),
            client_model="muse-spark-1.3-contributor-free",
            background_tasks=BackgroundTasks(),
            use_relay=False,  # langsung direct saja: 400 terlihat apa adanya
            opencode_headers={"x-opencode-session": "ses_TESTHEAL"},
        )
        chunks = []
        async for chunk in gen:
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(run())
    raw = "".join(chunks)
    # 1x gagal (heal), lalu sukses di percobaan berikutnya.
    assert len(fake.calls) == 2, [c["url"] for c in fake.calls]
    assert raw.find("healed!") != -1, raw[:500]
    # Payload attempt kedua TANPA item reasoning replay.
    second = fake.calls[1]["payload"]
    assert all(
        not (isinstance(i, dict) and i.get("type") == "reasoning")
        for i in second["input"]
    ), second["input"]
    # attempt pertama masih membawa reasoning replay (belum dibuang).
    first = fake.calls[0]["payload"]
    assert sum(1 for i in first["input"]
               if isinstance(i, dict) and i.get("type") == "reasoning") == 2


@case("D2 raw error 400 TIDAK diteruskan ke klien setelah heal sukses")
def _d2():
    import app.services.responses_bridge as rb
    from fastapi import BackgroundTasks

    fake = _HealFakeClient()
    _install_fake_client(None, fake)

    async def run():
        gen = rb.responses_stream_generator(
            _replay_payload(),
            client_model="muse-spark-1.3-contributor-free",
            background_tasks=BackgroundTasks(),
            use_relay=False,
        )
        return [c async for c in gen]

    chunks = asyncio.run(run())
    raw = "".join(chunks)
    assert "All responses targets failed" not in raw, raw[:300]
    assert "encrypted_content" not in raw or "healed!" in raw, raw[:300]


# ---------- E. heal via relay.error early-SSE ----------

@case("E1 relay HTTP 200 + relay.error 400 -> heal -> sukses")
def _e1():
    import app.services.responses_bridge as rb
    from fastapi import BackgroundTasks

    fake = _HealFakeClient(fail_with_sse_relay=True)
    _install_fake_client(None, fake)

    async def run():
        gen = rb.responses_stream_generator(
            _replay_payload(),
            client_model="muse-spark-1.3-contributor-free",
            background_tasks=BackgroundTasks(),
            use_relay=False,
        )
        return [c async for c in gen]

    chunks = asyncio.run(run())
    raw = "".join(chunks)
    assert len(fake.calls) == 2, [c["url"] for c in fake.calls]
    assert "healed!" in raw, raw[:500]
    second = fake.calls[1]["payload"]
    assert all(
        not (isinstance(i, dict) and i.get("type") == "reasoning")
        for i in second["input"]
    ), second["input"]


# ---------- F. guard: jangan over-heal ----------

@case("F1 error 400 BUKAN encrypted_content -> tidak heal (rotasi biasa)")
def _f1():
    import app.services.responses_bridge as rb
    from fastapi import BackgroundTasks

    class _Always400(_HealFakeClient):
        def _mk_response(self, payload_seen):
            return _FakeResponse(400, b'{"error":{"message":"bad model"}}')

    fake = _Always400()
    _install_fake_client(None, fake)

    async def run():
        gen = rb.responses_stream_generator(
            _replay_payload(),
            client_model="muse-spark-1.3-contributor-free",
            background_tasks=BackgroundTasks(),
            use_relay=False,
        )
        return [c async for c in gen]
    chunks = asyncio.run(run())
    raw = "".join(chunks)
    # 1 target (use_relay=False), gagal -> all-failed; TIDAK ada retry.
    assert len(fake.calls) == 1, [c["url"] for c in fake.calls]
    assert "All responses targets failed" in raw


class _AlwaysEncrypted(_HealFakeClient):
    def _mk_response(self, payload_seen):
        return _FakeResponse(400, _ENC_MSG.encode())


@case("F2 encrypted_content tapi payload TANPA reasoning replay -> tidak heal")
def _f2():
    import app.services.responses_bridge as rb
    from fastapi import BackgroundTasks

    fake = _AlwaysEncrypted()
    _install_fake_client(None, fake)
    plain = {"model": "m", "input": [{"role": "user",
            "content": [{"type": "input_text", "text": "hai"}]}]}

    async def run():
        gen = rb.responses_stream_generator(
            plain,
            client_model="muse-spark-1.3-contributor-free",
            background_tasks=BackgroundTasks(),
            use_relay=False,
        )
        return [c async for c in gen]

    chunks = asyncio.run(run())
    assert len(fake.calls) == 1, [c["url"] for c in fake.calls]
    assert "All responses targets failed" in "".join(chunks)


@case("F3 heal hanya SEKALI per request (gagal kedua -> all-failed)")
def _f3():
    import app.services.responses_bridge as rb
    from fastapi import BackgroundTasks

    class _AlwaysEncrypted(_HealFakeClient):
        def _mk_response(self, payload_seen):
            return _FakeResponse(400, _ENC_MSG.encode())

    fake = _AlwaysEncrypted()
    _install_fake_client(None, fake)

    async def run():
        gen = rb.responses_stream_generator(
            _replay_payload(),
            client_model="muse-spark-1.3-contributor-free",
            background_tasks=BackgroundTasks(),
            use_relay=False,
        )
        return [c async for c in gen]

    chunks = asyncio.run(run())
    # attempt 1 gagal -> heal -> attempt 2 masih gagal -> TIDAK heal lagi.
    assert len(fake.calls) == 2, [c["url"] for c in fake.calls]
    assert "All responses targets failed" in "".join(chunks)


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

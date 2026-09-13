"""Validasi fix sesi stabil per-percakapan (harus ALL PASSED).

Jalankan dari repo root:  python test/test_stable_session.py
Keluar dengan kode 0 bila semua passed, 1 bila ada yang gagal.

Latar: klien stateless (KiloCode) mereplay `reasoning.encrypted_content`
antara turn. Konten terenkripsi di-issuance ke caller identity (session
x-opencode-session). Sesi acak per-request membuat turn kedua ditolak
400 "encrypted_content was not issued to this caller" -> percakapan brick.

Cakupan:
  A. _conversation_fingerprint deterministik + sensitif isi
  B. _stable_opencode_session: prioritas env, fingerprint, fallback
  C. Header resolver: override sesi stabil; sesi klien dihormati
  D. Integrasi route: /v1/responses dan chat-bridge memakai sesi stabil
"""
import asyncio
import hashlib
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


# ---------- A. fingerprint ----------

@case("A1 fingerprint deterministik: payload sama -> hash sama")
def _a1():
    from app.services.opencode import _conversation_fingerprint
    body = {
        "model": "muse-spark-1.3-contributor-free",
        "instructions": "You are a helpful assistant.",
        "input": [
            {"role": "user", "content": [{"type": "input_text", "text": "halo dunia"}]},
        ],
        "store": False,
    }
    fp1 = _conversation_fingerprint(body)
    fp2 = _conversation_fingerprint(dict(body))
    assert fp1 and fp1 == fp2, (fp1, fp2)
    assert len(fp1) == 64, len(fp1)  # sha256 hex


@case("A2 prompt_cache_key eksplisit diutamakan")
def _a2():
    from app.services.opencode import _conversation_fingerprint
    a = _conversation_fingerprint({"prompt_cache_key": "conv-abc"})
    b = _conversation_fingerprint({"prompt_cache_key": "conv-abc", "input": "beda sekali"})
    c = _conversation_fingerprint({"prompt_cache_key": "conv-xyz"})
    assert a == b and a != c, (a, b, c)


@case("A3 chat messages (Hermes bridge) juga ter-fingerprint")
def _a3():
    from app.services.opencode import _conversation_fingerprint
    body = {
        "messages": [
            {"role": "system", "content": "sys prompt"},
            {"role": "user", "content": "pertanyaan pertama"},
            {"role": "assistant", "content": "jawaban"},
            {"role": "user", "content": "pertanyaan kedua"},
        ]
    }
    assert _conversation_fingerprint(body) == _conversation_fingerprint(dict(body))
    # Pesan user pertama tidak berubah antar-turn -> fingerprint stabil walau
    # history bertambah.
    longer = {
        "messages": body["messages"] + [{"role": "user", "content": "turn tiga"}]
    }
    assert _conversation_fingerprint(body) == _conversation_fingerprint(longer)


@case("A4 payload kosong/bukan-dict -> string kosong (bukan crash)")
def _a4():
    from app.services.opencode import _conversation_fingerprint
    assert _conversation_fingerprint(None) == ""
    assert _conversation_fingerprint({}) == ""
    assert _conversation_fingerprint("str") == ""
    assert _conversation_fingerprint({"input": [{"role": "user", "content": []}]}) == ""


# ---------- B. stable session ----------

@case("B1 env OPENCODE_SESSION_ID selalu menang")
def _b1():
    import app.services.opencode as oc
    old = oc.OPENCODE_SESSION_ID
    try:
        oc.OPENCODE_SESSION_ID = "ses_STATIK"
        assert oc._stable_opencode_session({"prompt_cache_key": "x"}) == "ses_STATIK"
        assert oc._stable_opencode_session(None) == "ses_STATIK"
    finally:
        oc.OPENCODE_SESSION_ID = old


@case("B2 fingerprint -> sesi stabil; percakapan beda -> sesi beda")
def _b2():
    import app.services.opencode as oc
    old = oc.OPENCODE_SESSION_ID
    try:
        oc.OPENCODE_SESSION_ID = ""
        conv1 = {"instructions": "sys", "input": [{"role": "user", "content": [{"type": "input_text", "text": "A"}]}]}
        conv2 = {"instructions": "sys", "input": [{"role": "user", "content": [{"type": "input_text", "text": "B"}]}]}
        s1a, s1b = oc._stable_opencode_session(conv1), oc._stable_opencode_session(dict(conv1))
        s2 = oc._stable_opencode_session(conv2)
        assert s1a == s1b, (s1a, s1b)
        assert s1a != s2, (s1a, s2)
        # Format ala CLI asli: ses_ + 26 alfanumerik campur huruf besar-kecil
        import string as _st
        assert s1a.startswith("ses_") and len(s1a) == 30, s1a
        assert all(c in _st.ascii_letters + _st.digits for c in s1a[4:]), s1a
    finally:
        oc.OPENCODE_SESSION_ID = old


@case("B2b format sesi identik CLI asli (alfanumerik campur, bukan hex)")
def _b2b():
    import app.services.opencode as oc
    import string as _st
    old = oc.OPENCODE_SESSION_ID
    try:
        oc.OPENCODE_SESSION_ID = ""
        for i in range(20):
            s = oc._stable_opencode_session({"prompt_cache_key": f"conv-{i}"})
            assert s.startswith("ses_") and len(s) == 30, s
            body = s[4:]
            assert all(c in _st.ascii_letters + _st.digits for c in body), s
            assert any(c.isupper() for c in body) or body == body.lower(), \
                f"semua lowercase terlihat seperti hex, bukan ala CLI: {s}"
    finally:
        oc.OPENCODE_SESSION_ID = old
@case("B3 tanpa sinyal -> fallback konstan (bukan acak per-call)")
def _b3():
    import app.services.opencode as oc
    old = oc.OPENCODE_SESSION_ID
    try:
        oc.OPENCODE_SESSION_ID = ""
        s1 = oc._stable_opencode_session({"model": "m"})
        s2 = oc._stable_opencode_session({"model": "m"})
        assert s1 == s2 and s1.startswith("ses_"), (s1, s2)
    finally:
        oc.OPENCODE_SESSION_ID = old


# ---------- C. resolver ----------

@case("C1 resolver tetap mengisi sesi acak bila klien tak kirim (perilaku lama)")
def _c1():
    from app.services.opencode import _resolve_opencode_headers
    h1 = _resolve_opencode_headers(None)
    h2 = _resolve_opencode_headers(None)
    assert h1["x-opencode-session"].startswith("ses_")
    assert h1["x-opencode-session"] != h2["x-opencode-session"]  # acak per-call


@case("C2 resolver menghormati sesi klien")
def _c2():
    from app.services.opencode import _resolve_opencode_headers
    h = _resolve_opencode_headers({"X-OpenCode-Session": "ses_KLIEN"})
    assert h["x-opencode-session"] == "ses_KLIEN"


# ---------- D. integrasi route ----------

@case("D1 /v1/responses: sesi stabil dipakai bila klien tidak mengirim")
def _d1():
    from app.routes import responses_api
    import app.services.opencode as oc

    class _Hdrs(dict):
        def get(self, k, default=None):  # case-insensitive ala starlette
            for key, v in dict.items(self):
                if key.lower() == k.lower():
                    return v
            return default

    captured = {}

    async def fake_call_upstream(body, **kw):
        captured["headers"] = kw.get("extra_headers")
        resp = type("R", (), {"status_code": 200, "text": "{}", "content": b"{}",
                              "json": lambda self: {"id": "x"}})()
        return resp, "direct"

    orig_call = responses_api.call_upstream
    orig_stable = oc.OPENCODE_SESSION_ID
    try:
        oc.OPENCODE_SESSION_ID = ""
        responses_api.call_upstream = fake_call_upstream
        req = type("Req", (), {"headers": _Hdrs({"content-type": "application/json"}),
                               "json": None})()

        async def fake_json():
            return {"model": "muse-spark", "instructions": "sys", "input": [
                {"role": "user", "content": [{"type": "input_text", "text": "u1"}]}]}

        req.json = fake_json
        asyncio.run(responses_api.create_response(req, None))
        s = captured["headers"]["x-opencode-session"]
        expected = oc._stable_opencode_session({
            "instructions": "sys",
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "u1"}]}],
        })
        assert s == expected, (s, expected)
        # Format ala CLI asli: ses_ + 26 alfanumerik campur huruf besar-kecil
        import string as _st
        assert len(s) == 30 and all(c in _st.ascii_letters + _st.digits for c in s[4:]), s
    finally:
        responses_api.call_upstream = orig_call
        oc.OPENCODE_SESSION_ID = orig_stable


@case("D2 chat-bridge: sesi stabil dipakai bila klien tidak mengirim")
def _d2():
    from app.routes import chat
    import app.services.opencode as oc
    from app.core.schemas import ChatCompletionRequest

    captured = {}

    async def fake_via_responses(req, client_model, background_tasks, headers):
        captured["headers"] = headers
        return "ok"

    orig = chat.chat_completions_via_responses
    old_stable = oc.OPENCODE_SESSION_ID
    try:
        oc.OPENCODE_SESSION_ID = ""
        chat.chat_completions_via_responses = fake_via_responses
        req = ChatCompletionRequest(
            model="muse-spark-1.3-contributor-free",
            messages=[{"role": "user", "content": "turn pertama"}],
        )
        class _Hdrs(dict):
            def get(self, k, default=None):
                return dict.get(self, k, default)
        request = type("Req", (), {"headers": _Hdrs({"content-type": "application/json"})})()
        asyncio.run(chat.chat_completions(req, None, request))
        s = captured["headers"]["x-opencode-session"]
        assert s.startswith("ses_") and len(s) == 30, s
        # Stabil: request kedua dengan isi sama -> sesi sama.
        captured.clear()
        asyncio.run(chat.chat_completions(req, None, request))
        assert captured["headers"]["x-opencode-session"] == s
    finally:
        chat.chat_completions_via_responses = orig
        oc.OPENCODE_SESSION_ID = old_stable


@case("D3 klien yang mengirim x-opencode-session tidak di-override")
def _d3():
    from app.routes import chat
    from app.core.schemas import ChatCompletionRequest

    captured = {}

    async def fake_via_responses(req, client_model, background_tasks, headers):
        captured["headers"] = headers
        return "ok"

    orig = chat.chat_completions_via_responses
    try:
        chat.chat_completions_via_responses = fake_via_responses
        req = ChatCompletionRequest(
            model="muse-spark-1.3-contributor-free",
            messages=[{"role": "user", "content": "halo"}],
        )
        request = type("Req", (), {"headers": {"x-opencode-session": "ses_KLIEN"}})()
        asyncio.run(chat.chat_completions(req, None, request))
        assert captured["headers"]["x-opencode-session"] == "ses_KLIEN"
    finally:
        chat.chat_completions_via_responses = orig


@case("E1 header CLI membawa User-Agent ala opencode asli")
def _e1():
    import app.services.opencode as oc
    h = oc._opencode_cli_headers()
    ua = h["User-Agent"]
    assert ua.startswith("opencode/"), ua
    assert "ai-sdk/" in ua and "runtime/" in ua, ua
    # Override env dihormati
    old = oc.OPENCODE_USER_AGENT
    try:
        oc.OPENCODE_USER_AGENT = "opencode/9.9.9 test"
        assert oc._opencode_cli_headers()["User-Agent"] == "opencode/9.9.9 test"
    finally:
        oc.OPENCODE_USER_AGENT = old


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

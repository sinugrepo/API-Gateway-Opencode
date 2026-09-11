"""Validasi fix long-stream / stream berulang (harus ALL PASSED).

Jalankan dari repo root:  python test/test_long_stream_fixes.py
Keluar dengan kode 0 bila semua passed, 1 bila ada yang gagal.

Cakupan:
  A. Missing import (bug crash): suppress + Union
  B. Relay state: prune dict, rotasi per-request, stream-limit, timeout detect, klasifikasi 429
  C. Config: relay CF hilang, normalisasi URL, nilai timeout waras
  D. Long-stream: idle timeout Responses = BRIDGE_REQUEST_TIMEOUT, reasoning buffer dibatasi
  E. Header identitas + SSE helper (pendukung stream stabil)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import inspect
import time
import traceback

import httpx

PASS, FAIL = "PASS", "FAIL"
_results = []


def case(name):
    def deco(fn):
        _results.append((name, fn))
        return fn
    return deco


# ---------- A. missing imports ----------

@case("A1 streaming module punya suppress (cleanup cancel tidak NameError)")
def _a1():
    import app.services.streaming as s
    assert hasattr(s, "suppress"), "suppress tidak di-import di streaming.py"
    # Pakai objek suppress MILIK modul untuk replika blok finally-nya.
    async def _cleanup():
        t = asyncio.create_task(asyncio.sleep(60))
        await asyncio.sleep(0)
        t.cancel()
        with s.suppress(asyncio.CancelledError, Exception):
            await t
        return True
    assert asyncio.run(_cleanup()) is True


@case("A2 responses_bridge punya suppress")
def _a2():
    import app.services.responses_bridge as b
    assert hasattr(b, "suppress"), "suppress tidak di-import di responses_bridge.py"

    async def _cleanup():
        t = asyncio.create_task(asyncio.sleep(60))
        await asyncio.sleep(0)
        t.cancel()
        with b.suppress(asyncio.CancelledError, Exception):
            await t
        return True
    assert asyncio.run(_cleanup()) is True


@case("A3 anotasi Union resolve + fungsi tool_choice benar")
def _a3():
    import app.services.responses_bridge as b
    ann = b._chat_tool_choice_to_responses.__annotations__  # NameError bila Union hilang
    assert "tool_choice" in ann
    assert b._chat_tool_choice_to_responses("auto") == "auto"
    assert b._chat_tool_choice_to_responses(None) is None
    assert b._chat_tool_choice_to_responses({"type": "function", "function": {"name": "x"}}) == {
        "type": "function", "name": "x"}


# ---------- B. relay state ----------

@case("B1 _relay_penalty prune entri kedaluwarsa (tidak bocor di long-running)")
def _b1():
    from app.services import relay as r
    with r._relay_penalty_lock:
        r._relay_penalty.clear()
        r._relay_penalty["https://stale-a.example/"] = time.time() - 10
        r._relay_penalty["https://stale-b.example/"] = time.time() - 1
        r._relay_penalty["https://fresh.example/"] = time.time() + 600
    r._mark_relay_rate_limited("https://new.example/", time.time() + 60)
    with r._relay_penalty_lock:
        assert "https://stale-a.example/" not in r._relay_penalty
        assert "https://stale-b.example/" not in r._relay_penalty
        assert "https://fresh.example/" in r._relay_penalty
        assert "https://new.example/" in r._relay_penalty
        r._relay_penalty.clear()


@case("B2 _relay_stream_broken prune entri kedaluwarsa")
def _b2():
    from app.services import relay as r
    with r._relay_stream_broken_lock:
        r._relay_stream_broken.clear()
        r._relay_stream_broken["https://stale.example/"] = time.time() - 5
    r._mark_relay_stream_broken("https://new.example/", time.time() + 60)
    with r._relay_stream_broken_lock:
        assert "https://stale.example/" not in r._relay_stream_broken
        assert "https://new.example/" in r._relay_stream_broken
        r._relay_stream_broken.clear()


@case("B3 rotasi per-request: request berurutan mulai dari relay berbeda")
def _b3():
    from app.services.relay import _relay_batch_for_request
    firsts = [_relay_batch_for_request()[0] for _ in range(4)]
    assert len(set(firsts)) > 1, f"rotasi macet: {firsts}"


@case("B4 relay kena 429 disusulkan ke akhir batch")
def _b4():
    from app.services import relay as r
    batch = r._relay_batch_for_request()
    victim = batch[0]
    r._mark_relay_rate_limited(victim, time.time() + 600)
    try:
        batch2 = r._relay_batch_for_request()
        assert batch2[-1] == victim, f"{victim} tidak di akhir: {batch2[-1]}"
    finally:
        with r._relay_penalty_lock:
            r._relay_penalty.clear()


@case("B5 stream-limit: relay dipotong, direct selalu dipertahankan")
def _b5():
    from app.core.config import MAX_RELAY_STREAM_ATTEMPTS
    from app.services.relay import _limit_stream_targets
    assert MAX_RELAY_STREAM_ATTEMPTS >= 0
    fake = [(f"https://r{i}.example/", {"x-relay-target": "x"}) for i in range(5)]
    fake.append(("https://direct.example/", {"Authorization": "y"}))
    out = _limit_stream_targets(list(fake))
    n_relay = sum(1 for _, h in out if "x-relay-target" in h)
    assert n_relay == MAX_RELAY_STREAM_ATTEMPTS, f"relay={n_relay}, max={MAX_RELAY_STREAM_ATTEMPTS}"
    assert "x-relay-target" not in out[-1][1], "direct harus terakhir"


@case("B6 _is_relay_timeout hanya 504 platform (500 upstream bukan salah relay)")
def _b6():
    from app.services.relay import _is_relay_timeout
    assert _is_relay_timeout(httpx.Response(504, headers={"x-vercel-error": "x"})), "504 vercel"
    assert _is_relay_timeout(httpx.Response(504, text="FUNCTION_INVOCATION_TIMEOUT")), "504 marker"
    assert not _is_relay_timeout(httpx.Response(500, text="internal server error")), "500 bukan timeout relay"
    assert not _is_relay_timeout(httpx.Response(429, text="rate limit")), "429 bukan timeout"
    assert not _is_relay_timeout(httpx.Response(200)), "200 bukan timeout"


@case("B7 _classify_rate_limit bedakan upstream vs vercel vs unknown")
def _b7():
    from app.services.upstream import _classify_rate_limit
    up = httpx.Response(429, json={"error": {"message": "rate_limit exceeded", "type": "rate_limit"}})
    assert _classify_rate_limit(up)["source"] == "upstream", "body rate_limit -> upstream"
    vc = httpx.Response(429, headers={"x-vercel-error": "TOO_MANY_REQUESTS"}, text="limit")
    assert _classify_rate_limit(vc)["source"] == "vercel", "header vercel -> vercel"
    un = httpx.Response(429, text="slow down please")
    assert _classify_rate_limit(un)["source"] == "unknown", "body asing -> unknown"


# ---------- C. config ----------

@case("C1 relay CF hilang, 11 relay Vercel ternormalisasi")
def _c1():
    from app.core.config import RELAY_URLS
    assert len(RELAY_URLS) == 11, f"jumlah relay={len(RELAY_URLS)}"
    for u in RELAY_URLS:
        assert "workers.dev" not in u, f"relay CF masih ada: {u}"
        assert u.startswith("https://"), u
        assert u.endswith("/api/relay"), u
    assert len(set(RELAY_URLS)) == len(RELAY_URLS), "ada relay duplikat"


@case("C2 _normalize_relay_url: hostname telanjang -> /api/relay")
def _c2():
    from app.core.config import _normalize_relay_url
    assert _normalize_relay_url("relay-x.vercel.app") == "https://relay-x.vercel.app/api/relay"
    assert _normalize_relay_url("https://a.example/api/relay") == "https://a.example/api/relay"
    assert _normalize_relay_url("") == ""
    assert _normalize_relay_url("  ") == ""


@case("C3 nilai timeout/cooldown waras untuk long-stream")
def _c3():
    from app.core.config import (BRIDGE_REQUEST_TIMEOUT, MAX_RELAY_STREAM_ATTEMPTS,
                                 RATE_LIMIT_COOLDOWN, RELAY_STREAM_BROKEN_COOLDOWN,
                                 REQUEST_TIMEOUT)
    assert REQUEST_TIMEOUT >= 60, REQUEST_TIMEOUT
    assert BRIDGE_REQUEST_TIMEOUT >= REQUEST_TIMEOUT, (BRIDGE_REQUEST_TIMEOUT, REQUEST_TIMEOUT)
    assert RATE_LIMIT_COOLDOWN >= 30, RATE_LIMIT_COOLDOWN
    assert RELAY_STREAM_BROKEN_COOLDOWN >= 600, RELAY_STREAM_BROKEN_COOLDOWN
    assert 0 <= MAX_RELAY_STREAM_ATTEMPTS <= 5, MAX_RELAY_STREAM_ATTEMPTS


# ---------- D. long-stream ----------

@case("D1 responses_stream_generator pakai BRIDGE_REQUEST_TIMEOUT (bukan 120s)")
def _d1():
    import app.services.responses_bridge as b
    assert not hasattr(b, "REQUEST_TIMEOUT"), "REQUEST_TIMEOUT masih diimpor/dipakai"
    src = inspect.getsource(b.responses_stream_generator)
    assert "BRIDGE_REQUEST_TIMEOUT" in src, "idle loop tidak memakai BRIDGE_REQUEST_TIMEOUT"
    code_lines = [ln for ln in src.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    bare = [ln for ln in code_lines if "REQUEST_TIMEOUT" in ln.replace("BRIDGE_REQUEST_TIMEOUT", "")]
    assert not bare, f"masih ada REQUEST_TIMEOUT telanjang: {bare}"


@case("D2 reasoning_buffer dibatasi (streaming.py + bridge)")
def _d2():
    import inspect
    import app.services.streaming as s
    import app.services.responses_bridge as b
    src_s = inspect.getsource(s.stream_generator)
    assert "reasoning_buffer_chars" in src_s and "pop(0)" in src_s, "cap reasoning chat hilang"
    assert "20000" in src_s, "batas 20rb char hilang"
    src_b = inspect.getsource(b.responses_to_chat_stream_generator)
    assert "reasoning_buffer_chars < 20000" in src_b, "cap reasoning bridge hilang"


@case("D3 bridge: buffered full-object hormati cap 20rb char")
def _d3():
    import inspect
    import app.services.responses_bridge as b
    src = inspect.getsource(b.responses_to_chat_stream_generator)
    assert "20000 - reasoning_buffer_chars" in src, "buffered path bisa tambah 20rb/event tanpa batas"


# ---------- E. pendukung stream stabil ----------

@case("E1 header stream matikan kompresi + bawa identitas CLI")
def _e1():
    from app.services.opencode import _resolve_opencode_headers
    from app.services.relay import _stream_request_headers
    oc = _resolve_opencode_headers(None)
    assert oc["x-opencode-session"].startswith("ses_") and len(oc["x-opencode-session"]) == 30
    assert oc["x-opencode-request"].startswith("msg_") and len(oc["x-opencode-request"]) == 28
    h = _stream_request_headers(oc)
    assert h["Accept-Encoding"] == "identity", "kompresi harus mati agar SSE tidak di-buffer"
    assert h["Accept"] == "text/event-stream"
    assert "x-opencode-session" in h


@case("E2 _sse hasilkan frame SSE valid")
def _e2():
    from app.core.sse import _sse
    frame = _sse({"hello": "world"})
    assert frame.startswith("data: ") and frame.endswith("\n\n"), repr(frame)


@case("F1 _is_giant_payload: kecil False, >100 item / >32KB True")
def _f1():
    from app.services.relay import _is_giant_payload
    assert _is_giant_payload({"model": "m", "input": "halo"}) is False
    assert _is_giant_payload({"input": [{"i": i} for i in range(101)]}) is True
    assert _is_giant_payload({"messages": [{"i": i} for i in range(101)]}) is True
    assert _is_giant_payload({"input": "x" * 40000}) is True
    assert _is_giant_payload(None) is False


@case("F2 _should_mark_stream_broken: giant dilewati, normal ditandai")
def _f2():
    from app.services import relay as r
    small = {"model": "m", "input": "halo"}
    giant = {"input": [{"i": i} for i in range(230)]}
    assert r._should_mark_stream_broken("https://x.example/", small) is True
    with r._relay_stream_broken_lock:
        r._relay_stream_broken.clear()
    assert r._should_mark_stream_broken("https://x.example/", giant) is False
    with r._relay_stream_broken_lock:
        # giant TIDAK boleh meninggalkan penanda (relay sehat jangan diracuni)
        assert "https://x.example/" not in r._relay_stream_broken
        r._relay_stream_broken.clear()


@case("G1 image_url dict -> input_image + detail diteruskan")
def _g1():
    from app.services.responses_bridge import _chat_messages_to_responses_input
    out = _chat_messages_to_responses_input([{
        "role": "user", "content": [
            {"type": "text", "text": "apa isi gambar?"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAA", "detail": "high"}},
        ]}])
    assert out[0]["role"] == "user"
    kinds = [p["type"] for p in out[0]["content"]]
    assert kinds == ["input_text", "input_image"], kinds
    img = out[0]["content"][1]
    assert img["image_url"] == "data:image/png;base64,AAA" and img["detail"] == "high"


@case("G2 image_url string -> detail auto; teks saja tak berubah")
def _g2():
    from app.services.responses_bridge import _chat_messages_to_responses_input
    out = _chat_messages_to_responses_input([{
        "role": "user", "content": [
            {"type": "image_url", "image_url": "https://x.example/a.png"},
        ]}])
    assert out[0]["content"][1] == {
        "type": "input_image", "image_url": "https://x.example/a.png", "detail": "auto"}
    plain = _chat_messages_to_responses_input([{"role": "user", "content": "halo"}])
    assert plain == [{"role": "user",
                      "content": [{"type": "input_text", "text": "halo"}]}]


@case("G3 bukan-list / tanpa gambar -> [] tanpa crash")
def _g3():
    from app.services.responses_bridge import _chat_image_contents
    assert _chat_image_contents("string") == []
    assert _chat_image_contents(None) == []
    assert _chat_image_contents([{"type": "text", "text": "x"}]) == []


@case("G4 total gambar raksasa -> HTTP 400 jelas (bukan 413 relay)")
def _g4():
    from fastapi import HTTPException
    from app.services.responses_bridge import _chat_messages_to_responses_input
    big = "data:image/png;base64," + "A" * 4_500_000  # ~3.3MB
    try:
        _chat_messages_to_responses_input([
            {"role": "user", "content": [{"type": "image_url",
                                          "image_url": {"url": big}}]}])
    except HTTPException as e:
        assert e.status_code == 400, e.status_code
    else:
        raise AssertionError("gambar raksasa lolos tanpa 400")


@case("H1 part file PDF -> input_file + filename dipertahankan")
def _h1():
    from app.services.responses_bridge import _chat_messages_to_responses_input
    out = _chat_messages_to_responses_input([{
        "role": "user", "content": [
            {"type": "text", "text": "baca pdf ini"},
            {"type": "file", "file": {
                "filename": "doc.pdf",
                "file_data": "data:application/pdf;base64,AAA"}},
        ]}])
    kinds = [p["type"] for p in out[0]["content"]]
    assert kinds == ["input_text", "input_file"], kinds
    f = out[0]["content"][1]
    assert f["filename"] == "doc.pdf"
    assert f["file_data"] == "data:application/pdf;base64,AAA"


@case("H2 file_id diteruskan; tanpa filename ditebak dari mime")
def _h2():
    from app.services.responses_bridge import _chat_messages_to_responses_input
    out = _chat_messages_to_responses_input([{
        "role": "user", "content": [
            {"type": "file", "file": {"file_id": "file-abc123"}},
            {"type": "file", "file": {"file_data": "data:application/pdf;base64,AAA"}},
        ]}])
    assert out[0]["content"][1] == {"type": "input_file", "file_id": "file-abc123"}
    assert out[0]["content"][2]["filename"] == "file.pdf"


@case("H3 budget gambar+file dipakai bersama -> kombinasi raksasa 400")
def _h3():
    from fastapi import HTTPException
    from app.services.responses_bridge import _chat_messages_to_responses_input
    img = "data:image/png;base64," + "A" * 2_500_000   # ~1.9MB
    pdf = "data:application/pdf;base64," + "B" * 2_500_000  # ~1.9MB
    try:
        _chat_messages_to_responses_input([{"role": "user", "content": [
            {"type": "image_url", "image_url": {"url": img}},
            {"type": "file", "file": {"filename": "d.pdf", "file_data": pdf}},
        ]}])
    except HTTPException as e:
        assert e.status_code == 400, e.status_code
    else:
        raise AssertionError("gambar raksasa lolos tanpa 400")


@case("J1 _payload_has_media: struktural, bukan substring")
def _j1():
    from app.services.relay import _payload_has_media as m
    assert m({"messages": [{"role": "user", "content": [
        {"type": "image_url", "image_url": {"url": "data:x"}}]}]}) is True
    assert m({"messages": [{"role": "user", "content": [
        {"type": "file", "file": {"filename": "a.pdf"}}]}]}) is True
    assert m({"input": [{"role": "user", "content": [
        {"type": "input_image", "image_url": "u"}]}]}) is True
    assert m({"input": [{"role": "user", "content": [
        {"type": "input_file", "file_id": "f"}]}]}) is True
    # Teks biasa yang menyebut kata image_url TIDAK boleh kena.
    assert m({"messages": [{"role": "user",
                            "content": "jelaskan image_url biasa"}]}) is False
    assert m({"messages": [{"role": "user", "content": "halo"}]}) is False
    assert m({"model": "m"}) is False
    assert m({}) is False
    assert m(None) is False


@case("J2 guard vision-429 ada di 3 generator stream + direct-first non-stream")
def _j2():
    import inspect
    import app.services.streaming as s
    import app.services.responses_bridge as b
    import app.services.upstream as u
    for fn in (s.stream_generator, b.responses_stream_generator,
               b.responses_to_chat_stream_generator):
        src = inspect.getsource(fn)
        assert "vision_direct_first" in src, fn.__name__
        assert "relay khusus 429" in src or "khusus 429" in src, fn.__name__
    assert "vision_direct_first" in inspect.getsource(u.call_upstream)


@case("H1 part file file_data+filename -> input_file utuh")
def _h1():
    from app.services.responses_bridge import _chat_messages_to_responses_input
    out = _chat_messages_to_responses_input([{
        "role": "user", "content": [
            {"type": "text", "text": "baca pdf ini"},
            {"type": "file", "file": {"filename": "doc.pdf",
                                      "file_data": "data:application/pdf;base64,AAA"}},
        ]}])
    kinds = [p["type"] for p in out[0]["content"]]
    assert kinds == ["input_text", "input_file"], kinds
    f = out[0]["content"][1]
    assert f["filename"] == "doc.pdf" and f["file_data"] == "data:application/pdf;base64,AAA"


@case("H2 file_id -> input_file by id; tanpa filename ditebak dari mime")
def _h2():
    from app.services.responses_bridge import _chat_media_contents
    got = _chat_media_contents([
        {"type": "file", "file": {"file_id": "file-abc123"}},
        {"type": "file", "file": {"file_data": "data:application/pdf;base64,AAA"}},
    ])
    assert got[0] == {"type": "input_file", "file_id": "file-abc123"}, got[0]
    assert got[1]["filename"] == "file.pdf", got[1]


@case("H3 gambar + file sejalan, budget dipakai bersama")
def _h3():
    from fastapi import HTTPException
    from app.services.responses_bridge import _chat_media_contents
    img = "data:image/png;base64," + "A" * 2_100_000  # ~1.57MB
    pdf = "data:application/pdf;base64," + "B" * 2_100_000  # total ~3.15MB > 3MB
    try:
        _chat_media_contents([
            {"type": "image_url", "image_url": {"url": img}},
            {"type": "file", "file": {"filename": "d.pdf", "file_data": pdf}},
        ])
    except HTTPException as e:
        assert e.status_code == 400, e.status_code
    else:
        raise AssertionError("kombinasi >3MB lolos tanpa 400")


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

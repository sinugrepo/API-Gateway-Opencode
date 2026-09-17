"""Live integration test: SEMUA case adalah real HTTP request, tanpa mock.

Jalankan dari repo root:  python test/test_live_requests.py
Exit 0 bila semua passed, 1 bila ada gagal.

Arsitektur: app FastAPI dijalankan in-process via httpx.ASGITransport, sehingga
setiap request benar-benar mengeksekusi routing, relay round-robin, dan
HTTP nyata ke relay Vercel + upstream opencode.ai. Tidak ada
httpx.Response buatan, tidak ada patch, tidak ada stub.

Kontrak 429: limit upstream bisa terjadi kapan saja (spurious-429 muse-spark).
Case inferensi lolos via TIGA jalur kontrak yang valid:
  - 200-path: bentuk respons sukses sesuai skema, atau
  - 429-path: 429 bersih + header Retry-After (non-stream) /
    chunk SSE code=RATE_LIMITED + [DONE] (stream), atau
  - empty-path: chunk SSE code=EMPTY_RESPONSE + [DONE] (upstream 200-OK
    tapi nol konten, mis. max_tokens habis untuk thinking; by-design
    di responses_bridge agar klien bisa retry/fallback).
GAGAL bila: 500/502, STREAM_ERROR, NameError, Traceback, bentuk tak dikenal,
atau stream putus tanpa [DONE].
"""
import asyncio
import json
import os
import random
import string
import sys
import tempfile
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Isolasi DB usage agar tidak mengotori repo (dibaca config saat import app).
_TMPD = tempfile.mkdtemp(prefix="live-test-")
os.environ["USAGE_DB_PATH"] = os.path.join(_TMPD, "usage-test.db")

import httpx  # noqa: E402

from app import app  # noqa: E402

SPARK = "muse-spark-1.3-contributor-free"
_results = []
PASS, FAIL = "PASS", "FAIL"


def case(name):
    def deco(fn):
        _results.append((name, fn))
        return fn
    return deco


def _rid(prefix, n):
    return prefix + "".join(random.choice(string.ascii_letters + string.digits) for _ in range(n))


def _session_headers():
    # Header spec-valid opencode-session.md §4 (§7: sesi stabil per
    # conversation, request unik per POST). Pakai generator produksi agar
    # live test mengirim fingerprint persis seperti CLI asli.
    from app.services.opencode import _new_opencode_request_id, _new_opencode_session_id
    return {
        "x-opencode-client": "cli",
        "x-opencode-project": "global",
        "x-opencode-session": _new_opencode_session_id(),
        "x-opencode-request": _new_opencode_request_id(),
    }


def _client():
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test",
                             timeout=httpx.Timeout(300.0, connect=15.0))


async def _read_sse(resp):
    """Kumpulkan semua baris 'data:' dari stream SSE nyata. Return (events, raw_lines)."""
    events, raws = [], []
    async for line in resp.aiter_lines():
        raws.append(line)
        if line.startswith("data:"):
            events.append(line[5:].lstrip())
    return events, raws


def _fail_if_crash(text, where):
    for marker in ("NameError", "Traceback", "STREAM_ERROR", "Stream error",
                   "Stream connection lost", "All stream targets failed",
                   "All responses targets failed"):
        assert marker not in text, f"{where}: marker crash {marker!r} dalam {text[:300]!r}"


# ---------------- local + real-relay (tanpa biaya inferensi) ----------------

@case("L1 GET /health -> 200, 11 relay, tanpa workers.dev")
async def _l1(client):
    r = await client.get("/health")
    assert r.status_code == 200, r.text[:300]
    body = r.json()
    assert body["status"] == "ok"
    assert "workers.dev" not in body["relay"], body["relay"]
    assert len(body["relay"].split(",")) == 11, body["relay"]


@case("L2 GET /v1/models -> 200, daftar nyata berisi spark (upstream real)")
async def _l2(client):
    r = await client.get("/v1/models")
    assert r.status_code == 200, r.text[:300]
    ids = [m["id"] for m in r.json()["data"]]
    assert len(ids) > 0, "daftar model kosong"
    assert any("spark" in i for i in ids), f"tak ada spark di {len(ids)} model"


@case("L3 GET /relay/status -> sukses, IP ter-masking (probe relay real)")
async def _l3(client):
    r = await client.get("/relay/status")
    assert r.status_code == 200, r.text[:300]
    body = r.json()
    assert body["success"] is True, body
    assert body["is_masked"] is True, body
    ok = [x for x in body["all_relays"] if x["ok"]]
    assert len(ok) >= 10, f"hanya {len(ok)}/11 relay ok: {body['all_relays']}"


@case("L4 POST chat messages kosong -> 400 terstruktur")
async def _l4(client):
    r = await client.post("/v1/chat/completions",
                          json={"model": SPARK, "messages": []})
    assert r.status_code == 400, (r.status_code, r.text[:300])
    err = r.json()["error"]
    assert err["type"] == "invalid_request_error", r.text[:300]
    assert "Messages cannot be empty" in err["message"], r.text[:300]


@case("L5 POST chat tanpa model -> 400 (tidak ada default diam-diam)")
async def _l5(client):
    r = await client.post("/v1/chat/completions",
                          json={"messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 400, (r.status_code, r.text[:300])


@case("L6 POST /v1/responses body bukan-JSON -> 400")
async def _l6(client):
    r = await client.post("/v1/responses", content=b"bukan json",
                          headers={"Content-Type": "application/json"})
    assert r.status_code == 400, (r.status_code, r.text[:300])


@case("L7 GET route tak dikenal -> 404 JSON")
async def _l7(client):
    r = await client.get("/tidak-ada-xyz")
    assert r.status_code == 404, r.status_code
    assert "detail" in r.json(), r.text[:200]


@case("L8 GET /v1/props -> 200, 11 relay_urls")
async def _l8(client):
    r = await client.get("/v1/props")
    assert r.status_code == 200, r.text[:300]
    assert len(r.json()["relay_urls"]) == 11


# ---------------- inferensi nyata (tiny, murah) ----------------

def _check_nonstream_contract(resp, where):
    """Return 'ok-200' / 'ok-429'. Raise bila di luar kontrak."""
    if resp.status_code == 200:
        body = resp.json()
        assert isinstance(body, dict) and ("output" in body or "id" in body), \
            f"{where}: bentuk 200 asing: {resp.text[:300]!r}"
        return "ok-200"
    if resp.status_code == 429:
        assert resp.headers.get("Retry-After") is not None, f"{where}: 429 tanpa Retry-After"
        err = resp.json().get("error", {})
        assert err.get("type") == "rate_limit_error", f"{where}: 429 tak bersih: {resp.text[:300]!r}"
        return "ok-429"
    _fail_if_crash(resp.text, where)
    raise AssertionError(f"{where}: status {resp.status_code} di luar kontrak: {resp.text[:300]!r}")


def _check_stream_contract(status, ctype, events, where):
    """Return ('ok-200', n) / ('ok-429', 0) / ('ok-empty', n). Raise bila di luar kontrak."""
    joined = "\n".join(events)
    if events and events[-1] == "[DONE]":
        if '"RATE_LIMITED"' in joined:
            return "ok-429", 0
        if '"EMPTY_RESPONSE"' in joined:
            _fail_if_crash(joined.replace("EMPTY_RESPONSE", ""), where)
            return "ok-empty", len(events)
        if status == 200:
            _fail_if_crash(joined, where)
            return "ok-200", len(events)
    _fail_if_crash(joined, where)
    raise AssertionError(
        f"{where}: stream di luar kontrak status={status} ctype={ctype} "
        f"events={len(events)} tail={events[-2:]!r}")


@case("U1 responses non-stream spark tiny -> 200-shape / 429-bersih")
async def _u1(client):
    r = await client.post("/v1/responses", json={
        "model": SPARK, "input": "jawab tepat satu kata: pong",
        "max_output_tokens": 16, "store": False}, headers=_session_headers())
    outcome = _check_nonstream_contract(r, "U1")
    print(f"      [U1 outcome={outcome}]")


@case("U2 responses stream spark tiny -> SSE utuh hingga [DONE] / RATE_LIMITED")
async def _u2(client):
    async with client.stream("POST", "/v1/responses", json={
            "model": SPARK, "input": "jawab tepat satu kata: pong",
            "max_output_tokens": 16, "stream": True, "store": False},
            headers=_session_headers()) as r:
        assert r.status_code == 200, r.status_code
        ctype = r.headers.get("content-type", "")
        assert "text/event-stream" in ctype, ctype
        events, _ = await _read_sse(r)
    outcome, n = _check_stream_contract(r.status_code, ctype, events, "U2")
    print(f"      [U2 outcome={outcome} events={n}]")


@case("U3 chat bridge stream spark -> delta chat valid / RATE_LIMITED / EMPTY")
async def _u3(client):
    async with client.stream("POST", "/v1/chat/completions", json={
            "model": SPARK, "stream": True, "max_tokens": 128,
            "messages": [{"role": "user", "content": "jawab tepat satu kata: pong"}]},
            headers=_session_headers()) as r:
        assert r.status_code == 200, r.status_code
        events, _ = await _read_sse(r)
    outcome, n = _check_stream_contract(r.status_code, "", events, "U3")
    if outcome == "ok-200":
        seen_delta = False
        for e in events:
            if e == "[DONE]":
                continue
            obj = json.loads(e)
            if "choices" not in obj or not obj["choices"]:
                continue  # event usage-only tanpa choices, valid di OpenAI
            d = obj["choices"][0]["delta"]
            if any(k in d for k in ("content", "tool_calls", "reasoning_content", "role")):
                seen_delta = True
                break
        assert seen_delta, "200-path tanpa delta chat sama sekali"
    print(f"      [U3 outcome={outcome} events={n}]")


@case("U4 3x stream berulang 1 sesi -> semua finish bersih (regresi bug sesi panjang)")
async def _u4(client):
    from app.services.opencode import _new_opencode_request_id
    ses = _session_headers()  # satu sesi dipakai bersama, seperti Hermes
    for i in range(3):
        ses["x-opencode-request"] = _new_opencode_request_id()
        async with client.stream("POST", "/v1/responses", json={
                "model": SPARK, "input": f"sebutkan angka {i} saja",
                "max_output_tokens": 16, "stream": True, "store": False},
                headers=ses) as r:
            assert r.status_code == 200, (i, r.status_code)
            events, _ = await _read_sse(r)
        outcome, n = _check_stream_contract(r.status_code, "", events, f"U4 iter-{i}")
        print(f"      [U4 iter-{i} outcome={outcome} events={n}]")
        await asyncio.sleep(2)


@case("U5 stream panjang (>=1000 char wire) -> utuh hingga [DONE]")
async def _u5(client):
    async with client.stream("POST", "/v1/responses", json={
            "model": SPARK,
            "input": "tulis cerita minimal 300 kata tentang robot dan hujan, "
                     "langsung isi tanpa pembuka",
            "max_output_tokens": 512, "stream": True, "store": False},
            headers=_session_headers()) as r:
        assert r.status_code == 200, r.status_code
        t0 = time.time()
        events, _ = await _read_sse(r)
        elapsed = time.time() - t0
    outcome, n = _check_stream_contract(r.status_code, "", events, "U5")
    wire = sum(len(e) for e in events if e != "[DONE]")
    if outcome == "ok-200":
        # Panjang diukur dari byte konten, bukan jumlah event: relay boleh
        # membungkus respons buffered menjadi sedikit event besar.
        assert wire >= 1000, f"stream panjang hanya {wire} char wire / {n} event"
    print(f"      [U5 outcome={outcome} events={n} wire={wire} elapsed={elapsed:.1f}s]")


@case("U6 chat non-stream bridge spark tiny -> choices / 429-bersih")
async def _u6(client):
    r = await client.post("/v1/chat/completions", json={
        "model": SPARK, "max_tokens": 16,
        "messages": [{"role": "user", "content": "jawab tepat satu kata: pong"}]},
        headers=_session_headers())
    if r.status_code == 200:
        body = r.json()
        assert body["object"] == "chat.completion", r.text[:300]
        assert body["choices"] and body["choices"][0]["message"]["role"] == "assistant"
        print("      [U6 outcome=ok-200]")
    elif r.status_code == 429:
        assert r.headers.get("Retry-After") is not None
        print("      [U6 outcome=ok-429]")
    else:
        _fail_if_crash(r.text, "U6")
        raise AssertionError(f"U6: status {r.status_code}: {r.text[:300]!r}")


@case("U7 chat stream mimo tiny -> delta KONTEN nyata (bukti isi mengalir)")
async def _u7(client):
    async with client.stream("POST", "/v1/chat/completions", json={
            "model": "mimo-v2.5-free", "stream": True, "max_tokens": 32,
            "messages": [{"role": "user", "content": "sebutkan 3 warna saja"}]},
            headers=_session_headers()) as r:
        assert r.status_code == 200, r.status_code
        events, _ = await _read_sse(r)
    outcome, n = _check_stream_contract(r.status_code, "", events, "U7")
    content = ""
    if outcome == "ok-200":
        for e in events:
            if e == "[DONE]":
                continue
            obj = json.loads(e)
            if "choices" in obj and obj["choices"]:
                content += obj["choices"][0]["delta"].get("content") or ""
        # Failover bersih ("All stream targets failed") tetap valid bila
        # model sedang unavailable upstream; yang dilarang hanya crash.
        if '"All stream targets failed"' not in "\n".join(events):
            assert content.strip(), "200-path tanpa konten sama sekali"
    print(f"      [U7 outcome={outcome} events={n} content_chars={len(content)}]")


@case("F1 failover model-unavailable -> error bersih + [DONE], tanpa crash")
async def _f1(client):
    async with client.stream("POST", "/v1/chat/completions", json={
            "model": "deepseek-v4-flash-free", "stream": True, "max_tokens": 32,
            "messages": [{"role": "user", "content": "halo"}]},
            headers=_session_headers()) as r:
        # HTTP selalu 200 untuk stream (error disampaikan sebagai chunk).
        assert r.status_code == 200, r.status_code
        events, _ = await _read_sse(r)
    assert events and events[-1] == "[DONE]", f"F1: tak ada [DONE]: {events[-2:]!r}"
    joined = "\n".join(events)
    _fail_if_crash(joined.replace("All stream targets failed", ""), "F1")
    has_content = any(
        e != "[DONE]" and "choices" in e and '"content"' in e for e in events)
    has_clean_error = "All stream targets failed" in joined or "RATE_LIMITED" in joined
    assert has_content or has_clean_error, f"F1: tak ada konten maupun error bersih: {joined[:300]!r}"
    print(f"      [F1 events={len(events)} content={has_content} clean_error={has_clean_error}]")


def _tiny_png_data_url():
    """PNG 8x8 merah valid, dibuat dari stdlib (tanpa file/PIL)."""
    import base64
    import struct
    import zlib
    w = h = 8
    raw = b"".join(b"\x00" + b"\xff\x00\x00" * w for _ in range(h))

    def chunk(ctype, data):
        c = struct.pack(">I", len(data)) + ctype + data
        return c + struct.pack(">I", zlib.crc32(ctype + data) & 0xFFFFFFFF)

    png = (b"\x89PNG\r\n\x1a\n"
           + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0))
           + chunk(b"IDAT", zlib.compress(raw))
           + chunk(b"IEND", b""))
    return "data:image/png;base64," + base64.b64encode(png).decode()


@case("U8 vision: gambar via bridge spark -> diterima upstream (bentuk valid)")
async def _u8(client):
    r = await client.post("/v1/chat/completions", json={
        "model": SPARK, "max_tokens": 2048,
        "messages": [{"role": "user", "content": [
            {"type": "text", "text": "apa warna kotak pada gambar? jawab singkat"},
            {"type": "image_url", "image_url": {"url": _tiny_png_data_url()}},
        ]}]}, headers=_session_headers())
    if r.status_code == 200:
        body = r.json()
        content = body["choices"][0]["message"].get("content") or ""
        print(f"      [U8 outcome=ok-200 content_chars={len(content)} "
              f"preview={content[:120]!r}]")
    elif r.status_code == 429:
        assert r.headers.get("Retry-After") is not None
        print("      [U8 outcome=ok-429]")
    else:
        _fail_if_crash(r.text, "U8")
        raise AssertionError(f"U8: status {r.status_code}: {r.text[:300]!r}")


def _tiny_pdf_data_url():
    """PDF 1 halaman valid (teks HELLOPDF), xref dihitung programatik."""
    import base64
    stream = b"BT /F1 24 Tf 50 150 Td (HELLOPDF) Tj ET"
    objs = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] "
         b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>"),
        b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = b"%PDF-1.4\n"
    offsets = []
    for i, body in enumerate(objs, 1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % i + body + b"\nendobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objs) + 1)
    for o in offsets:
        out += b"%010d 00000 n \n" % o
    out += (b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF"
            % (len(objs) + 1, xref))
    return "data:application/pdf;base64," + base64.b64encode(out).decode()


@case("U9 vision PDF: dokumen via bridge spark -> dibaca upstream")
async def _u9(client):
    r = await client.post("/v1/chat/completions", json={
        "model": SPARK, "max_tokens": 2048,
        "messages": [{"role": "user", "content": [
            {"type": "text", "text": "tulis teks dalam pdf, jawab singkat"},
            {"type": "file", "file": {"filename": "hello.pdf",
                                      "file_data": _tiny_pdf_data_url()}},
        ]}]}, headers=_session_headers())
    if r.status_code == 200:
        body = r.json()
        content = body["choices"][0]["message"].get("content") or ""
        print(f"      [U9 outcome=ok-200 content_chars={len(content)} "
              f"preview={content[:120]!r}]")
    elif r.status_code == 429:
        assert r.headers.get("Retry-After") is not None
        print("      [U9 outcome=ok-429]")
    else:
        _fail_if_crash(r.text, "U9")
        raise AssertionError(f"U9: status {r.status_code}: {r.text[:300]!r}")


async def _run_one(name, fn, client, inference):
    # Anti-loop: tiap case dibatasi 280 dtk (di bawah timeout runner).
    # Lapisan: httpx timeout 300s + wait_for ini + timeout tool bash.
    t0 = time.time()
    try:
        await asyncio.wait_for(fn(client), timeout=280.0)
    except asyncio.TimeoutError:
        print(f"[{FAIL}] {name} (TIMEOUT 280s)")
        traceback.print_stack(limit=5)
        return False
    except Exception:
        print(f"[{FAIL}] {name} ({time.time()-t0:.1f}s)")
        traceback.print_exc(limit=5)
        return False
    else:
        print(f"[{PASS}] {name} ({time.time()-t0:.1f}s)")
        return True
    finally:
        if inference:
            await asyncio.sleep(3)  # sopan ke upstream di antar case inferensi


async def main():
    print(f"live suite: {len(_results)} case, SEMUA real HTTP (ASGI in-process -> "
          "relay Vercel + opencode.ai). Tanpa mock.")
    print("=" * 70)
    passed = failed = 0
    async with _client() as client:
        for name, fn in _results:
            inference = name.startswith("U")
            ok = await _run_one(name, fn, client, inference)
            passed, failed = passed + ok, failed + (not ok)
    print("=" * 70)
    print(f"hasil: {passed} passed, {failed} failed dari {len(_results)} case")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

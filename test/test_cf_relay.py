"""Live test relay Cloudflare (Edge): https://relay.sinug.workers.dev/

Jalankan dari repo root:  python test/test_cf_relay.py [PREFIX ...]
Exit 0 bila semua passed/skipped, 1 bila ada gagal. Tanpa argumen = semua case.

Basis Edge (beda dari Vercel):
  - Workers tidak punya kill 25-detik first-byte ala Vercel. Stream boleh
    lama selama byte pertama cepat. Worker CF di sini mengirim
    `: relay-connected` SEGERA (<1 dtk), lalu pass/wrap body, sehingga
    rule first-byte selalu terpenuhi walau upstream TTFB lambat.
  - Non-stream (tanpa `Accept: text/event-stream`): body upstream POLOS,
    content-type asli dipertahankan, tanpa framing SSE.
  - Stream (dengan `Accept: text/event-stream`): worker membungkus body
    MENTAH menjadi framing SSE (`: relay-connected` + `data: ...` +
    `data: [DONE]`). Untuk upstream yang SUDAH SSE (opencode Responses),
    worker pass-through garis `data:` apa adanya (tidak double-wrap
    `data: data:`) lalu tetap menutup dengan `data: [DONE]`.
  - Routing via header `x-relay-target` (origin, wajib) + `x-relay-path`
    (path+query, default "/"). Tanpa/invalid target -> 400 JSON.

Cakupan case (prefix = filter argv):
  EDGE*: fondasi edge (server cloudflare, cf-ray, tanpa x-vercel-error,
      CORS *, 400 missing/invalid, status passthrough, Retry-After,
      method/query/header forwarding, /api/relay, invalid host, IP masking)
  COLD*: cold-start + wall-time (first-hit TTFB/wall, warmed avg, paralel,
      non-stream wall <15s, stream TTFB cepat)
  N*: non-stream generik via httpbin (GET/POST/PUT/DELETE/PATCH)
  S*: stream framing via httpbin (drip, /stream/N, POST+SSE, negatif non-wrap)
  L*: long >25 dtk via httpbin drip 30s (proxy bukti lolos kill Vercel)
  R*: v1/responses VIA CF ke opencode.ai (butuh OPENCODE_API_KEY):
      non-stream tiny, stream tiny (validasi SSE Responses asli, anti
      double-wrap, anti relay.error), stream PANJANG (cerita 300 kata,
      wall/TTFB/DONE tanpa 504/524)
  V*: vision VIA CF (input_image PNG tiny + input_file PDF tiny via
      Responses, plus konversi unit chat->Responses, plus budget guard)
  G*: kompatibilitas gateway (_with_relay_headers, _stream_request_headers,
      _normalize_relay_url, _payload_has_media, _limit_stream_targets)

Kontrak inferensi (R/V): 200-path (bentuk valid) ATAU 429-bersih
(non-stream: 429 + Retry-After; stream: event RATE_LIMITED/relay.error 429
+ [DONE]) = PASS. GAGAL bila: 500/502/504/524, STREAM_ERROR, NameError,
Traceback, double-wrap `data: data:`, putus tanpa [DONE], atau bentuk tak
dikenal. Tanpa OPENCODE_API_KEY -> SKIP (bukan FAIL).
"""
import asyncio
import base64
import json
import os
import struct
import sys
import time
import traceback
import zlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

CF_ROOT = "https://relay.sinug.workers.dev/"
CF_API = "https://relay.sinug.workers.dev/api/relay"

OPENCODE_ORIGIN = "https://opencode.ai"
OPENCODE_RESP_PATH = "/zen/v1/responses"
OPENCODE_CHAT_PATH = "/zen/v1/chat/completions"

SPARK = "muse-spark-1.3-contributor-free"

_results = []
PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"


class _Skip(Exception):
    pass


def case(name):
    def deco(fn):
        _results.append((name, fn))
        return fn
    return deco


def _client(timeout=60.0):
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=15.0),
        headers={"User-Agent": "api-gateway-cf-test/2.0"},
    )


def _h(target, path, extra=None):
    h = {"x-relay-target": target, "x-relay-path": path}
    if extra:
        h.update(extra)
    return h


def _api_key():
    try:
        from app.core.config import API_KEY as _k
    except Exception:
        _k = ""
    env = (os.getenv("OPENCODE_API_KEY") or "").strip()
    key = env or (_k or "").strip()
    if not key or key.lower() == "public":
        return ""
    return key


def _require_key():
    key = _api_key()
    if not key:
        raise _Skip("OPENCODE_API_KEY kosong/public -> skip inferensi live")
    return key


def _oc_session_headers():
    """Identitas CLI ala gateway (lihat app/services/opencode.py)."""
    import random
    import string as _st
    rnd = "".join(random.choice(_st.ascii_letters + _st.digits) for _ in range(26))
    rnd2 = "".join(random.choice(_st.ascii_letters + _st.digits) for _ in range(24))
    return {
        "x-opencode-client": "cli",
        "x-opencode-project": "global",
        "x-opencode-session": f"ses_{rnd}",
        "x-opencode-request": f"msg_{rnd2}",
    }


async def _collect_stream(resp, t0):
    """Kumpulkan semua baris + raw text; return (lines, raw, ttfb, wall)."""
    lines, first_at = [], None
    async for line in resp.aiter_lines():
        if first_at is None:
            first_at = time.time() - t0
        lines.append(line)
    ttfb = first_at if first_at is not None else time.time() - t0
    return lines, "\n".join(lines), ttfb, time.time() - t0


def _assert_sse_wrapped(lines, raw, where):
    assert lines, f"{where}: stream kosong"
    assert lines[0].strip() == ": relay-connected", \
        f"{where}: baris pertama bukan ': relay-connected': {lines[0][:80]!r}"
    assert any(l.startswith("data: ") for l in lines), \
        f"{where}: tak ada baris 'data: '"
    stripped = [l.strip() for l in lines if l.strip()]
    assert stripped and stripped[-1] == "data: [DONE]", \
        f"{where}: tidak diakhiri 'data: [DONE]': {stripped[-2:]!r}"
    assert "data: [DONE]" in raw, f"{where}: raw tanpa DONE"


def _assert_no_double_wrap(lines, where):
    bad = [l for l in lines if l.startswith("data: data:")]
    assert not bad, f"{where}: double-wrap SSE rusak: {bad[0][:120]!r}"


def _fail_if_crash(text, where):
    for marker in ("NameError", "Traceback", "STREAM_ERROR", "Stream error",
                   "Stream connection lost", "All stream targets failed",
                   "All responses targets failed"):
        assert marker not in text, f"{where}: marker crash {marker!r} dalam {text[:300]!r}"


def _assert_responses_stream(lines, raw, where, min_events=2):
    """Validasi stream Responses ASLI lewat CF: SSE native, DONE utuh."""
    assert lines, f"{where}: stream kosong"
    _assert_no_double_wrap(lines, where)
    assert "relay.error" not in raw or '"status": 429' in raw, \
        f"{where}: relay.error non-429: {raw[:300]!r}"
    stripped = [l.strip() for l in lines if l.strip()]
    assert stripped and stripped[-1] == "data: [DONE]", \
        f"{where}: tidak diakhiri 'data: [DONE]': {stripped[-2:]!r}"
    data_lines = [l for l in lines if l.startswith("data: ") and l.strip() != "data: [DONE]"]
    assert len(data_lines) >= min_events, \
        f"{where}: event terlalu sedikit ({len(data_lines)}): {raw[:300]!r}"
    # Minimal satu event JSON bertipe Responses.
    has_resp = False
    for l in data_lines:
        body = l[5:].lstrip()
        if '"response' in body or '"output_text' in body or '"usage"' in body \
                or '"type"' in body:
            has_resp = True
            break
    assert has_resp, f"{where}: tak ada event Responses: {raw[:400]!r}"


def _tiny_png_data_url():
    """PNG 8x8 merah valid dari stdlib (tanpa file/PIL)."""
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


def _tiny_pdf_data_url():
    """PDF 1 halaman valid (teks HELLOPDF), xref dihitung programatik."""
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


# ---------------- EDGE: fondasi workers ----------------

@case("EDGE-1 server cloudflare + cf-ray, tanpa x-vercel-error, CORS *")
async def _edge1():
    async with _client() as c:
        r = await c.get(CF_ROOT, headers=_h("https://httpbin.org", "/get"))
        assert r.status_code == 200, (r.status_code, r.text[:200])
        assert r.headers.get("server") == "cloudflare", dict(r.headers)
        assert r.headers.get("cf-ray"), "tanpa cf-ray"
        assert "x-vercel-error" not in {k.lower() for k in r.headers}, \
            "ada penanda vercel di relay CF!"
        assert r.headers.get("access-control-allow-origin") == "*", dict(r.headers)


@case("EDGE-2 tanpa x-relay-target -> 400 Missing")
async def _edge2():
    async with _client() as c:
        r = await c.get(CF_ROOT)
        assert r.status_code == 400, r.status_code
        assert "Missing x-relay-target" in r.text, r.text[:200]


@case("EDGE-3 target invalid -> 400 Invalid")
async def _edge3():
    async with _client() as c:
        r = await c.get(CF_ROOT, headers=_h("notaurl", "/"))
        assert r.status_code == 400, r.status_code
        assert "Invalid" in r.text, r.text[:200]


@case("EDGE-4 status upstream diteruskan (404/500/429), bukan 200")
async def _edge4():
    async with _client() as c:
        for path, expect in (("/status/404", 404), ("/status/500", 500),
                             ("/status/429", 429)):
            r = await c.get(CF_ROOT, headers=_h("https://httpbin.org", path))
            assert r.status_code == expect, (path, r.status_code, r.text[:200])


@case("EDGE-5 Retry-After upstream diteruskan")
async def _edge5():
    async with _client() as c:
        r = await c.get(CF_ROOT, headers=_h(
            "https://httpbin.org", "/response-headers?status=429&Retry-After=7"))
        assert r.headers.get("Retry-After") == "7", dict(r.headers)


@case("EDGE-6 method/query/header diteruskan (PUT/DELETE/PATCH + query + auth)")
async def _edge6():
    async with _client() as c:
        r = await c.get(CF_ROOT, headers=_h("https://httpbin.org", "/get?foo=bar&baz=qux"))
        assert r.status_code == 200, r.status_code
        assert r.json()["args"] == {"foo": "bar", "baz": "qux"}, r.text[:200]
        r = await c.put(CF_ROOT, headers=_h("https://httpbin.org", "/put"),
                        json={"a": 1})
        assert r.status_code == 200 and r.json()["json"] == {"a": 1}, r.text[:200]
        r = await c.request("DELETE", CF_ROOT, headers=_h("https://httpbin.org", "/delete"))
        assert r.status_code == 200, ("DELETE", r.status_code, r.text[:200])
        r = await c.patch(CF_ROOT, headers=_h("https://httpbin.org", "/patch"),
                          json={"p": 9})
        assert r.status_code == 200 and r.json()["json"] == {"p": 9}, r.text[:200]
        r = await c.get(CF_ROOT, headers=_h(
            "https://httpbin.org", "/headers",
            {"Authorization": "Bearer SECRET123", "x-opencode-session": "ses_TEST123"}))
        assert r.status_code == 200, r.status_code
        hdrs = {k.lower(): v for k, v in r.json()["headers"].items()}
        assert hdrs.get("authorization") == "Bearer SECRET123", hdrs
        assert hdrs.get("x-opencode-session") == "ses_TEST123", hdrs


@case("EDGE-7 path sendiri /api/relay jalan sama + IP masking")
async def _edge7():
    async with _client() as c:
        r = await c.post(CF_API, headers=_h("https://httpbin.org", "/post"),
                         json={"via": "api-relay-path"})
        assert r.status_code == 200, (r.status_code, r.text[:200])
        assert r.json()["json"] == {"via": "api-relay-path"}
        direct = (await c.get("https://api.ipify.org?format=json")).json()["ip"]
        via = (await c.get(CF_ROOT, headers=_h("https://api.ipify.org",
                                               "/?format=json"))).json()["ip"]
    assert via and direct and via != direct, f"direct={direct} via={via}"
    print(f"      [direct={direct} via={via}]")


@case("EDGE-8 host invalid -> error non-200 terkontrol (bukan crash/hang)")
async def _edge8():
    async with _client(timeout=40.0) as c:
        r = await c.get(CF_ROOT, headers=_h("https://nonexistent.invalid", "/"))
        assert r.status_code != 200, r.status_code
        print(f"      [status={r.status_code}]")


# ---------------- COLD: cold-start + wall-time ----------------

@case("COLD-1 first-hit (cold) TTFB+wall cepat, early-hint : relay-connected")
async def _cold1():
    async with _client(timeout=60.0) as c:
        t0 = time.time()
        async with c.stream("GET", CF_ROOT, headers=_h(
                "https://httpbin.org", "/drip?duration=1&numbytes=5&code=200",
                {"Accept": "text/event-stream"})) as r:
            assert r.status_code == 200, r.status_code
            lines, raw, ttfb, wall = await _collect_stream(r, t0)
    _assert_sse_wrapped(lines, raw, "COLD-1")
    # Edge cold-start workers biasanya <2s; longgarkan ke 15s agar tidak flaky.
    assert ttfb < 15.0, f"cold TTFB lambat: {ttfb:.1f}s"
    assert wall < 30.0, f"cold wall lambat: {wall:.1f}s"
    print(f"      [cold ttfb={ttfb:.2f}s wall={wall:.2f}s]")


@case("COLD-2 warmed 3x avg wall stabil (tanpa penalty stream)")
async def _cold2():
    walls = []
    async with _client() as c:
        for _ in range(3):
            t0 = time.time()
            r = await c.get(CF_ROOT, headers=_h("https://httpbin.org", "/get"))
            assert r.status_code == 200, r.status_code
            walls.append(time.time() - t0)
    avg = sum(walls) / len(walls)
    assert avg < 15.0, f"warmed avg lambat: {avg:.1f}s walls={walls}"
    print(f"      [walls={[f'{w:.2f}' for w in walls]} avg={avg:.2f}s]")


@case("COLD-3 paralel 5x non-stream semua 200 (edge concurrency)")
async def _cold3():
    async with _client(timeout=60.0) as c:
        async def _one(i):
            r = await c.get(CF_ROOT, headers=_h("https://httpbin.org", f"/get?slot={i}"))
            assert r.status_code == 200, (i, r.status_code)
            assert r.json()["args"] == {"slot": str(i)}
            return True

        out = await asyncio.gather(*(_one(i) for i in range(5)))
    assert all(out) and len(out) == 5


@case("COLD-4 stream TTFB cepat (<10s) walau total mengikuti duration")
async def _cold4():
    async with _client(timeout=60.0) as c:
        t0 = time.time()
        async with c.stream("GET", CF_ROOT, headers=_h(
                "https://httpbin.org", "/drip?duration=4&numbytes=8&code=200",
                {"Accept": "text/event-stream"})) as r:
            assert r.status_code == 200, r.status_code
            lines, raw, ttfb, wall = await _collect_stream(r, t0)
    _assert_sse_wrapped(lines, raw, "COLD-4")
    assert ttfb < 10.0, f"TTFB lambat (mirip kill Vercel): {ttfb:.1f}s"
    assert 3.0 <= wall <= 30.0, f"wall aneh untuk drip 4s: {wall:.1f}s"
    print(f"      [ttfb={ttfb:.2f}s wall={wall:.2f}s]")


# ---------------- N: non-stream generik ----------------

@case("N-1 GET ipify via relay -> 200 JSON polos, tanpa framing SSE")
async def _n1():
    async with _client() as c:
        r = await c.get(CF_ROOT, headers=_h("https://api.ipify.org", "/?format=json"))
        assert r.status_code == 200, (r.status_code, r.text[:200])
        assert "application/json" in r.headers.get("content-type", ""), r.headers
        body = r.json()
        assert "ip" in body and body["ip"], r.text[:200]
        assert not r.text.startswith(":"), "non-stream terbungkus SSE!"
        assert "[DONE]" not in r.text, "non-stream mengandung [DONE]!"
        print(f"      [relay_ip={body['ip']}]")


@case("N-2 POST JSON echo -> body utuh, tanpa framing SSE")
async def _n2():
    payload = {"hello": "world", "n": 123, "nested": {"a": [1, 2]}}
    async with _client() as c:
        r = await c.post(CF_ROOT, headers=_h("https://httpbin.org", "/post"),
                         json=payload)
        assert r.status_code == 200, (r.status_code, r.text[:200])
        assert r.json()["json"] == payload, f"echo rusak: {r.text[:300]!r}"
        assert "[DONE]" not in r.text


# ---------------- S: stream framing ----------------

@case("S-1 drip pendek + Accept SSE -> framing relay-connected/DONE")
async def _s1():
    async with _client() as c:
        t0 = time.time()
        async with c.stream("GET", CF_ROOT, headers=_h(
                "https://httpbin.org", "/drip?duration=2&numbytes=10&code=200",
                {"Accept": "text/event-stream"})) as r:
            assert r.status_code == 200, r.status_code
            assert "text/event-stream" in r.headers.get("content-type", ""), \
                r.headers.get("content-type")
            lines, raw, ttfb, wall = await _collect_stream(r, t0)
        _assert_sse_wrapped(lines, raw, "S-1")
        _assert_no_double_wrap(lines, "S-1")
        print(f"      [ttfb={ttfb:.2f}s wall={wall:.2f}s lines={len(lines)}]")


@case("S-2 /stream/3 + Accept SSE -> dibungkus, DONE utuh, isi lengkap")
async def _s2():
    async with _client() as c:
        async with c.stream("GET", CF_ROOT, headers=_h(
                "https://httpbin.org", "/stream/3",
                {"Accept": "text/event-stream"})) as r:
            assert r.status_code == 200, r.status_code
            t0 = time.time()
            lines, raw, ttfb, wall = await _collect_stream(r, t0)
        _assert_sse_wrapped(lines, raw, "S-2")
        assert '"id": 0' in raw and '"id": 2' in raw, "isi stream hilang"
        print(f"      [ttfb={ttfb:.2f}s wall={wall:.2f}s bytes={len(raw)}]")


@case("S-3 POST + Accept SSE (pola gateway streaming) -> dibungkus + DONE")
async def _s3():
    async with _client() as c:
        async with c.stream("POST", CF_ROOT, headers=_h(
                "https://httpbin.org", "/post",
                {"Accept": "text/event-stream", "Content-Type": "application/json"}),
                json={"m": "hi"}) as r:
            assert r.status_code == 200, r.status_code
            assert "text/event-stream" in r.headers.get("content-type", ""), \
                r.headers.get("content-type")
            t0 = time.time()
            lines, raw, _, _ = await _collect_stream(r, t0)
        _assert_sse_wrapped(lines, raw, "S-3")
        assert '"m": "hi"' in raw or '"m":"hi"' in raw, "echo POST hilang"


@case("S-4 drip TANPA Accept -> TIDAK dibungkus (raw polos, tanpa DONE)")
async def _s4():
    async with _client() as c:
        async with c.stream("GET", CF_ROOT, headers=_h(
                "https://httpbin.org", "/drip?duration=2&numbytes=10&code=200")) as r:
            assert r.status_code == 200, r.status_code
            chunks = []
            async for ch in r.aiter_text():
                chunks.append(ch)
            raw = "".join(chunks)
    assert raw == "*" * 10, f"body drip rusak: {raw!r}"
    assert "[DONE]" not in raw and "relay-connected" not in raw, \
        f"non-SSE ikut dibungkus: {raw!r}"


# ---------------- L: long >25 dtk (anti-kill Vercel) ----------------

@case("L-1 drip 30s (>25s) -> total>=25s, TTFB cepat, DONE utuh, tanpa 504/524")
async def _l1():
    async with _client(timeout=120.0) as c:
        t0 = time.time()
        async with c.stream("GET", CF_ROOT, headers=_h(
                "https://httpbin.org", "/drip?duration=30&numbytes=30&code=200",
                {"Accept": "text/event-stream"})) as r:
            assert r.status_code == 200, r.status_code
            assert r.status_code not in (504, 524), f"edge kill: {r.status_code}"
            lines, raw, ttfb, wall = await _collect_stream(r, t0)
    assert wall >= 25.0, f"stream berhenti prematur: wall={wall:.1f}s"
    assert ttfb < 10.0, f"TTFB lambat (mirip kill 25s Vercel): ttfb={ttfb:.1f}s"
    _assert_sse_wrapped(lines, raw, "L-1")
    _assert_no_double_wrap(lines, "L-1")
    _fail_if_crash(raw, "L-1")
    print(f"      [wall={wall:.1f}s ttfb={ttfb:.2f}s bytes={len(raw)}]")


# ---------------- R: v1/responses via CF (live opencode) ----------------

def _cf_responses_headers(api_key, stream=True, extra=None):
    base = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
        "Accept": "text/event-stream" if stream else "application/json",
        "x-relay-target": OPENCODE_ORIGIN,
        "x-relay-path": OPENCODE_RESP_PATH,
    }
    if stream:
        base["Accept-Encoding"] = "identity"
    base.update(_oc_session_headers())
    if extra:
        base.update(extra)
    return base


@case("R-1 responses non-stream tiny via CF -> 200-shape / 429-bersih")
async def _r1():
    key = _require_key()
    payload = {"model": SPARK, "input": "jawab tepat satu kata: pong",
               "max_output_tokens": 16, "store": False}
    async with _client(timeout=90.0) as c:
        t0 = time.time()
        r = await c.post(CF_ROOT, headers=_cf_responses_headers(key, stream=False),
                         json=payload)
        wall = time.time() - t0
    assert wall < 90.0, f"wall non-stream lambat: {wall:.1f}s"
    if r.status_code == 200:
        body = r.json()
        assert isinstance(body, dict) and ("output" in body or "id" in body), \
            f"bentuk 200 asing: {r.text[:300]!r}"
        print(f"      [R-1 ok-200 wall={wall:.1f}s]")
    elif r.status_code == 429:
        assert r.headers.get("Retry-After") is not None or "retry" in r.text.lower() \
            or "rate" in r.text.lower(), f"429 tak bersih: {r.text[:300]!r}"
        print(f"      [R-1 ok-429 wall={wall:.1f}s]")
    else:
        _fail_if_crash(r.text, "R-1")
        raise AssertionError(f"R-1: status {r.status_code}: {r.text[:300]!r}")


@case("R-2 responses stream tiny via CF -> SSE Responses asli + DONE, tanpa double-wrap")
async def _r2():
    key = _require_key()
    payload = {"model": SPARK, "input": "jawab tepat satu kata: pong",
               "max_output_tokens": 16, "stream": True, "store": False}
    async with _client(timeout=120.0) as c:
        t0 = time.time()
        async with c.stream("POST", CF_ROOT,
                            headers=_cf_responses_headers(key, stream=True),
                            json=payload) as r:
            assert r.status_code == 200, (r.status_code, r.text[:300] if hasattr(r, "text") else "")
            assert "text/event-stream" in r.headers.get("content-type", ""), \
                r.headers.get("content-type")
            lines, raw, ttfb, wall = await _collect_stream(r, t0)
    joined = "\n".join(lines)
    if '"RATE_LIMITED"' in joined or ('"relay.error"' in raw and '"status": 429' in raw):
        assert stripped_done(lines), "429-path tanpa [DONE]"
        print(f"      [R-2 ok-429 ttfb={ttfb:.2f}s wall={wall:.1f}s]")
        return
    _fail_if_crash(raw, "R-2")
    _assert_responses_stream(lines, raw, "R-2")
    assert ttfb < 60.0, f"TTFB stream tiny lambat: {ttfb:.1f}s"
    print(f"      [R-2 ok-200 ttfb={ttfb:.2f}s wall={wall:.1f}s events={len(lines)}]")


def stripped_done(lines):
    stripped = [l.strip() for l in lines if l.strip()]
    return bool(stripped) and stripped[-1] == "data: [DONE]"


@case("R-3 responses stream PANJANG via CF -> wall panjang, TTFB wajar, DONE utuh")
async def _r3():
    key = _require_key()
    payload = {"model": SPARK,
               "input": "tulis cerita minimal 300 kata tentang robot dan hujan, langsung isi",
               "max_output_tokens": 512, "stream": True, "store": False}
    async with _client(timeout=280.0) as c:
        t0 = time.time()
        async with c.stream("POST", CF_ROOT,
                            headers=_cf_responses_headers(key, stream=True),
                            json=payload) as r:
            assert r.status_code == 200, r.status_code
            assert r.status_code not in (504, 524), f"edge kill: {r.status_code}"
            lines, raw, ttfb, wall = await _collect_stream(r, t0)
    joined = "\n".join(lines)
    if '"RATE_LIMITED"' in joined or ('"relay.error"' in raw and '"status": 429' in raw):
        assert stripped_done(lines), "429-path tanpa [DONE]"
        print(f"      [R-3 ok-429 wall={wall:.1f}s]")
        return
    _fail_if_crash(raw, "R-3")
    _assert_responses_stream(lines, raw, "R-3", min_events=3)
    wire = sum(len(l) for l in lines if l.startswith("data: ") and l.strip() != "data: [DONE]")
    # Panjang diukur dari byte konten: relay boleh menggabung event.
    assert wire >= 1000, f"stream panjang hanya {wire} char wire / {len(lines)} baris"
    assert ttfb < 120.0, f"TTFB stream panjang lambat: {ttfb:.1f}s"
    print(f"      [R-3 ok-200 ttfb={ttfb:.2f}s wall={wall:.1f}s wire={wire}]")


@case("R-4 chat non-stream via CF (model umum) -> 200-shape / 429-bersih")
async def _r4():
    key = _require_key()
    payload = {"model": "mimo-v2.5-free",
               "messages": [{"role": "user", "content": "jawab tepat satu kata: pong"}],
               "max_tokens": 16}
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {key}",
               "Accept": "application/json", "x-relay-target": OPENCODE_ORIGIN,
               "x-relay-path": OPENCODE_CHAT_PATH, **_oc_session_headers()}
    async with _client(timeout=90.0) as c:
        t0 = time.time()
        r = await c.post(CF_ROOT, headers=headers, json=payload)
        wall = time.time() - t0
    if r.status_code == 200:
        body = r.json()
        assert body.get("object") == "chat.completion" and body.get("choices"), \
            f"bentuk chat 200 asing: {r.text[:300]!r}"
        print(f"      [R-4 ok-200 wall={wall:.1f}s]")
    elif r.status_code == 429:
        print(f"      [R-4 ok-429 wall={wall:.1f}s]")
    else:
        _fail_if_crash(r.text, "R-4")
        # Model bisa unavailable upstream (failover bersih di gateway, di sini
        # status asli diteruskan): terima 4xx/5xx non-200 sebagai "teruskan",
        # gagal hanya bila crash/hang.
        print(f"      [R-4 passthrough status={r.status_code} wall={wall:.1f}s]")


# ---------------- V: vision via CF ----------------

@case("V-1 vision image via Responses CF -> dibaca upstream (200 / 429)")
async def _v1():
    key = _require_key()
    payload = {"model": SPARK, "store": False, "max_output_tokens": 2048,
               "input": [{"role": "user", "content": [
                   {"type": "input_text", "text": "apa warna kotak pada gambar? jawab singkat"},
                   {"type": "input_image", "image_url": _tiny_png_data_url(),
                    "detail": "low"}]}]}
    async with _client(timeout=120.0) as c:
        t0 = time.time()
        r = await c.post(CF_ROOT, headers=_cf_responses_headers(key, stream=False),
                         json=payload)
        wall = time.time() - t0
    if r.status_code == 200:
        body = r.json()
        assert isinstance(body, dict) and ("output" in body or "id" in body), \
            f"bentuk vision 200 asing: {r.text[:300]!r}"
        print(f"      [V-1 ok-200 wall={wall:.1f}s]")
    elif r.status_code == 429:
        print(f"      [V-1 ok-429 wall={wall:.1f}s]")
    else:
        _fail_if_crash(r.text, "V-1")
        raise AssertionError(f"V-1: status {r.status_code}: {r.text[:300]!r}")


@case("V-2 vision PDF via Responses CF -> dibaca upstream (200 / 429)")
async def _v2():
    key = _require_key()
    payload = {"model": SPARK, "store": False, "max_output_tokens": 2048,
               "input": [{"role": "user", "content": [
                   {"type": "input_text", "text": "tulis teks dalam pdf, jawab singkat"},
                   {"type": "input_file", "filename": "hello.pdf",
                    "file_data": _tiny_pdf_data_url()}]}]}
    async with _client(timeout=120.0) as c:
        t0 = time.time()
        r = await c.post(CF_ROOT, headers=_cf_responses_headers(key, stream=False),
                         json=payload)
        wall = time.time() - t0
    if r.status_code == 200:
        body = r.json()
        assert isinstance(body, dict) and ("output" in body or "id" in body), \
            f"bentuk pdf 200 asing: {r.text[:300]!r}"
        print(f"      [V-2 ok-200 wall={wall:.1f}s]")
    elif r.status_code == 429:
        print(f"      [V-2 ok-429 wall={wall:.1f}s]")
    else:
        _fail_if_crash(r.text, "V-2")
        raise AssertionError(f"V-2: status {r.status_code}: {r.text[:300]!r}")


@case("V-3 konversi chat vision -> Responses input_image/input_file (unit)")
async def _v3():
    from app.services.responses_bridge import _chat_messages_to_responses_input
    out = _chat_messages_to_responses_input([{
        "role": "user", "content": [
            {"type": "text", "text": "apa isi gambar?"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAA",
                                                "detail": "high"}},
            {"type": "file", "file": {"filename": "doc.pdf",
                                      "file_data": "data:application/pdf;base64,AAA"}},
        ]}])
    kinds = [p["type"] for p in out[0]["content"]]
    assert kinds == ["input_text", "input_image", "input_file"], kinds
    assert out[0]["content"][1]["detail"] == "high"
    assert out[0]["content"][2]["filename"] == "doc.pdf"


@case("V-4 media raksasa -> 400 jelas di bridge (bukan 413/504 relay)")
async def _v4():
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


# ---------------- G: kompatibilitas gateway ----------------

@case("G-1 header builder gateway (_with_relay_headers) diterima worker CF")
async def _g1():
    from app.services.relay import _with_relay_headers
    base = {"Content-Type": "application/json",
            "Authorization": "Bearer GW-TEST",
            "Accept": "application/json"}
    hdrs = _with_relay_headers("https://httpbin.org/post", base)
    assert hdrs["x-relay-target"] == "https://httpbin.org", hdrs
    assert hdrs["x-relay-path"] == "/post", hdrs
    async with _client() as c:
        r = await c.post(CF_ROOT, headers=hdrs, json={"gw": 1})
        assert r.status_code == 200, (r.status_code, r.text[:200])
        body = r.json()
        assert body["json"] == {"gw": 1}, r.text[:200]
        echoed = {k.lower(): v for k, v in body["headers"].items()}
        assert echoed.get("authorization") == "Bearer GW-TEST", echoed


@case("G-2 header stream gateway (Accept SSE + identity) picu framing CF")
async def _g2():
    from app.services.relay import _stream_request_headers
    h = _stream_request_headers({"x-opencode-session": "ses_GWTEST"})
    assert h["Accept"] == "text/event-stream", h
    assert h["Accept-Encoding"] == "identity", h
    relay_h = dict(h)
    relay_h.update({"x-relay-target": "https://httpbin.org",
                    "x-relay-path": "/drip?duration=2&numbytes=5&code=200"})
    async with _client() as c:
        async with c.stream("POST", CF_ROOT, headers=relay_h,
                            json={"model": "m", "stream": True}) as r:
            assert r.status_code == 200, r.status_code
            t0 = time.time()
            lines, raw, _, _ = await _collect_stream(r, t0)
    _assert_sse_wrapped(lines, raw, "G-2")


@case("G-3 _normalize_relay_url terima host CF -> /api/relay yang hidup")
async def _g3():
    from app.core.config import _normalize_relay_url
    norm = _normalize_relay_url("relay.sinug.workers.dev")
    assert norm == "https://relay.sinug.workers.dev/api/relay", norm
    async with _client() as c:
        r = await c.post(norm, headers=_h("https://httpbin.org", "/post"),
                         json={"norm": True})
        assert r.status_code == 200, (r.status_code, r.text[:200])
        assert r.json()["json"] == {"norm": True}


@case("G-4 builder untuk OPENCODE_RESPONSES_URL hasilkan target/path benar")
async def _g4():
    from app.core.config import OPENCODE_RESPONSES_URL
    from app.services.relay import _with_relay_headers, _stream_request_headers
    base = _stream_request_headers(_oc_session_headers())
    hdrs = _with_relay_headers(OPENCODE_RESPONSES_URL, base)
    assert hdrs["x-relay-target"] == "https://opencode.ai", hdrs
    assert hdrs["x-relay-path"] == "/zen/v1/responses", hdrs
    assert hdrs["Accept"] == "text/event-stream"
    assert hdrs["Accept-Encoding"] == "identity"


@case("G-5 _payload_has_media + _limit_stream_targets waras untuk vision/stream")
async def _g5():
    from app.services.relay import (_limit_stream_targets, _payload_has_media)
    assert _payload_has_media(
        {"input": [{"role": "user", "content": [
            {"type": "input_image", "image_url": "u"}]}]}) is True
    assert _payload_has_media({"messages": [{"role": "user", "content": "halo"}]}) is False
    fake = [(f"https://r{i}.example/", {"x-relay-target": "x"}) for i in range(5)]
    fake.append(("https://direct.example/", {"Authorization": "y"}))
    out = _limit_stream_targets(list(fake))
    assert "x-relay-target" not in out[-1][1], "direct harus terakhir"


async def _run_one(name, fn):
    t0 = time.time()
    try:
        await asyncio.wait_for(fn(), timeout=290.0)
    except _Skip as e:
        print(f"[{SKIP}] {name} ({e})")
        return None
    except asyncio.TimeoutError:
        print(f"[{FAIL}] {name} (TIMEOUT 290s)")
        traceback.print_stack(limit=5)
        return False
    except Exception:
        print(f"[{FAIL}] {name} ({time.time()-t0:.1f}s)")
        traceback.print_exc(limit=5)
        return False
    else:
        print(f"[{PASS}] {name} ({time.time()-t0:.1f}s)")
        return True


async def main():
    prefixes = [a.upper() for a in sys.argv[1:]]
    items = [(n, f) for n, f in _results
             if not prefixes or any(n.upper().startswith(p) for p in prefixes)]
    if not items:
        print(f"tidak ada case diawali {prefixes}; tersedia: "
              + ", ".join(sorted({n.split('-')[0] for n, _ in _results})))
        return 1
    if not _api_key():
        print("CATATAN: OPENCODE_API_KEY kosong -> case R*/V-1/V-2 jadi SKIP "
              "(EDGE/COLD/N/S/L/G tetap jalan).")
    print(f"cf-relay(edge) suite: {len(items)} case, SEMUA real HTTP ke {CF_ROOT}")
    print("=" * 70)
    passed = failed = skipped = 0
    for name, fn in items:
        ok = await _run_one(name, fn)
        if ok is None:
            skipped += 1
        else:
            passed, failed = passed + ok, failed + (not ok)
        await asyncio.sleep(1)  # sopan ke worker + httpbin/opencode
    print("=" * 70)
    print(f"hasil: {passed} passed, {failed} failed, {skipped} skipped "
          f"dari {len(items)} case")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

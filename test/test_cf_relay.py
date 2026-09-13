"""Live test relay Cloudflare: https://relay.sinug.workers.dev/

Jalankan dari repo root:  python test/test_cf_relay.py
Exit 0 bila semua passed, 1 bila ada gagal.

Protokol worker CF (hasil probe 2026-09-13):
  - Routing via header: `x-relay-target` (origin, wajib) + `x-relay-path`
    (path+query, default "/"). Tanpa target -> 400 {"error": ...}.
  - Target invalid -> 400 {"error":"Invalid x-relay-target/x-relay-path"}.
  - Method/body/query/headers (termasuk Authorization) diteruskan apa adanya.
  - Status upstream diteruskan (404/500/429), bukan dibungkus 200.
  - Non-stream (tanpa `Accept: text/event-stream`): body upstream POLOS,
    content-type asli dipertahankan, tanpa framing SSE.
  - Stream (dengan `Accept: text/event-stream`): worker membungkus body
    menjadi framing SSE: diawali komentar `: relay-connected`, isi
    didahului `data: `, diakhiri `data: [DONE]`, content-type
    `text/event-stream` (chunked). Beda dari relay Vercel yang pass-through.
  - Kunci vs Vercel: stream 30 dtk (drip duration=30) selesai 200 dalam
    ~30.5 dtk dengan TTFB ~0.1 dtk -> TIDAK kena kill 25 dtk ala Vercel
    (first-byte rule). Inilah alasan CF cocok untuk stream panjang.

Cakupan case:
  N*: non-stream (GET/POST/PUT/DELETE/PATCH, query, header-forward, /api/relay)
  S*: stream (drip pendek, /stream/N, POST+SSE ala gateway, non-wrap negatif)
  L*: long >25 detik (drip 30s: total>=25s + TTFB cepat + DONE utuh)
  E*: error/lain-lain (400 missing/invalid, passthrough 4xx/5xx, Retry-After,
      CORS, server cloudflare, IP masking, invalid host)
  G*: kompatibilitas gateway (header builder _with_relay_headers,
      _stream_request_headers, _normalize_relay_url)
"""
import asyncio
import os
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

CF_ROOT = "https://relay.sinug.workers.dev/"
CF_API = "https://relay.sinug.workers.dev/api/relay"

_results = []
PASS, FAIL = "PASS", "FAIL"


def case(name):
    def deco(fn):
        _results.append((name, fn))
        return fn
    return deco


def _client(timeout=60.0):
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=15.0),
        headers={"User-Agent": "api-gateway-cf-test/1.0"},
    )


def _h(target, path, extra=None):
    h = {"x-relay-target": target, "x-relay-path": path}
    if extra:
        h.update(extra)
    return h


async def _collect_stream(resp, t0):
    """Kumpulkan semua baris + raw text; return (lines, raw, ttfb)."""
    lines, first_at = [], None
    async for line in resp.aiter_lines():
        if first_at is None:
            first_at = time.time() - t0
        lines.append(line)
    ttfb = first_at if first_at is not None else time.time() - t0
    return lines, "\n".join(lines), ttfb


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


# ---------------- N: non-stream ----------------

@case("CF-N1 GET ipify via relay -> 200 JSON polos, IP ter-masking")
async def _n1():
    async with _client() as c:
        r = await c.get(CF_ROOT, headers=_h("https://api.ipify.org", "/?format=json"))
        assert r.status_code == 200, (r.status_code, r.text[:200])
        assert "application/json" in r.headers.get("content-type", ""), r.headers
        body = r.json()
        assert "ip" in body and body["ip"], r.text[:200]
        raw = r.text
        assert not raw.startswith(":"), "non-stream terbungkus SSE!"
        assert "[DONE]" not in raw, "non-stream mengandung [DONE]!"
        print(f"      [relay_ip={body['ip']}]")


@case("CF-N2 POST JSON echo -> body utuh, tanpa framing SSE")
async def _n2():
    payload = {"hello": "world", "n": 123, "nested": {"a": [1, 2]}}
    async with _client() as c:
        r = await c.post(CF_ROOT, headers=_h("https://httpbin.org", "/post"),
                         json=payload)
        assert r.status_code == 200, (r.status_code, r.text[:200])
        body = r.json()
        assert body["json"] == payload, f"echo rusak: {r.text[:300]!r}"
        assert "[DONE]" not in r.text


@case("CF-N3 query string diteruskan utuh")
async def _n3():
    async with _client() as c:
        r = await c.get(CF_ROOT, headers=_h("https://httpbin.org", "/get?foo=bar&baz=qux"))
        assert r.status_code == 200, r.status_code
        args = r.json()["args"]
        assert args == {"foo": "bar", "baz": "qux"}, args


@case("CF-N4 PUT/DELETE/PATCH diteruskan dengan method asli")
async def _n4():
    async with _client() as c:
        r = await c.put(CF_ROOT, headers=_h("https://httpbin.org", "/put"),
                        json={"a": 1})
        assert r.status_code == 200, ("PUT", r.status_code, r.text[:200])
        assert r.json()["json"] == {"a": 1}
        r = await c.request("DELETE", CF_ROOT, headers=_h("https://httpbin.org", "/delete"))
        assert r.status_code == 200, ("DELETE", r.status_code, r.text[:200])
        r = await c.patch(CF_ROOT, headers=_h("https://httpbin.org", "/patch"),
                          json={"p": 9})
        assert r.status_code == 200, ("PATCH", r.status_code, r.text[:200])
        assert r.json()["json"] == {"p": 9}


@case("CF-N5 Authorization + header kustom diteruskan ke upstream")
async def _n5():
    async with _client() as c:
        r = await c.get(CF_ROOT, headers=_h(
            "https://httpbin.org", "/headers",
            {"Authorization": "Bearer SECRET123",
             "x-opencode-session": "ses_TEST123"}))
        assert r.status_code == 200, r.status_code
        hdrs = {k.lower(): v for k, v in r.json()["headers"].items()}
        assert hdrs.get("authorization") == "Bearer SECRET123", hdrs
        assert hdrs.get("x-opencode-session") == "ses_TEST123", hdrs


@case("CF-N6 path sendiri /api/relay (normalisasi gateway) jalan sama")
async def _n6():
    payload = {"via": "api-relay-path"}
    async with _client() as c:
        r = await c.post(CF_API, headers=_h("https://httpbin.org", "/post"),
                         json=payload)
        assert r.status_code == 200, (r.status_code, r.text[:200])
        assert r.json()["json"] == payload


# ---------------- S: stream ----------------

@case("CF-S1 stream drip pendek + Accept SSE -> framing relay-connected/DONE")
async def _s1():
    async with _client() as c:
        t0 = time.time()
        async with c.stream("GET", CF_ROOT, headers=_h(
                "https://httpbin.org", "/drip?duration=2&numbytes=10&code=200",
                {"Accept": "text/event-stream"})) as r:
            assert r.status_code == 200, r.status_code
            assert "text/event-stream" in r.headers.get("content-type", ""), \
                r.headers.get("content-type")
            lines, raw, ttfb = await _collect_stream(r, t0)
        _assert_sse_wrapped(lines, raw, "S1")
        print(f"      [ttfb={ttfb:.2f}s lines={len(lines)}]")


@case("CF-S2 stream /stream/3 + Accept SSE -> dibungkus, DONE utuh")
async def _s2():
    async with _client() as c:
        async with c.stream("GET", CF_ROOT, headers=_h(
                "https://httpbin.org", "/stream/3",
                {"Accept": "text/event-stream"})) as r:
            assert r.status_code == 200, r.status_code
            t0 = time.time()
            lines, raw, ttfb = await _collect_stream(r, t0)
        _assert_sse_wrapped(lines, raw, "S2")
        assert '"id": 0' in raw and '"id": 2' in raw, "isi stream hilang"
        print(f"      [ttfb={ttfb:.2f}s bytes={len(raw)}]")


@case("CF-S3 POST + Accept SSE (pola gateway streaming) -> dibungkus + DONE")
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
            lines, raw, _ = await _collect_stream(r, t0)
        _assert_sse_wrapped(lines, raw, "S3")
        assert '"m": "hi"' in raw or '"m":"hi"' in raw, "echo POST hilang"


@case("CF-S4 drip TANPA Accept -> TIDAK dibungkus (raw polos, tanpa DONE)")
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


# ---------------- L: long >25 detik ----------------

@case("CF-L1 stream drip 30s (>25s) -> total>=25s, TTFB cepat, DONE utuh")
async def _l1():
    async with _client(timeout=120.0) as c:
        t0 = time.time()
        async with c.stream("GET", CF_ROOT, headers=_h(
                "https://httpbin.org", "/drip?duration=30&numbytes=30&code=200",
                {"Accept": "text/event-stream"})) as r:
            assert r.status_code == 200, r.status_code
            lines, raw, ttfb = await _collect_stream(r, t0)
        total = time.time() - t0
    assert total >= 25.0, f"stream berhenti prematur: total={total:.1f}s"
    assert ttfb < 10.0, f"TTFB lambat (mirip kill 25s Vercel): ttfb={ttfb:.1f}s"
    _assert_sse_wrapped(lines, raw, "L1")
    print(f"      [total={total:.1f}s ttfb={ttfb:.2f}s bytes={len(raw)}]")


@case("CF-L2 non-stream cepat tetap <15s (tidak kena penalty stream)")
async def _l2():
    async with _client() as c:
        t0 = time.time()
        r = await c.get(CF_ROOT, headers=_h("https://httpbin.org", "/get"))
        total = time.time() - t0
        assert r.status_code == 200, r.status_code
    assert total < 15.0, f"non-stream lambat: {total:.1f}s"
    print(f"      [total={total:.2f}s]")


# ---------------- E: error + lain-lain ----------------

@case("CF-E1 tanpa x-relay-target -> 400 Missing")
async def _e1():
    async with _client() as c:
        r = await c.get(CF_ROOT)
        assert r.status_code == 400, r.status_code
        assert "Missing x-relay-target" in r.text, r.text[:200]


@case("CF-E2 target invalid -> 400 Invalid")
async def _e2():
    async with _client() as c:
        r = await c.get(CF_ROOT, headers=_h("notaurl", "/"))
        assert r.status_code == 400, r.status_code
        assert "Invalid" in r.text, r.text[:200]


@case("CF-E3 status upstream diteruskan (404/500/429), bukan 200")
async def _e3():
    async with _client() as c:
        for path, expect in (("/status/404", 404), ("/status/500", 500),
                             ("/status/429", 429)):
            r = await c.get(CF_ROOT, headers=_h("https://httpbin.org", path))
            assert r.status_code == expect, (path, r.status_code, r.text[:200])


@case("CF-E4 header Retry-After upstream diteruskan")
async def _e4():
    async with _client() as c:
        r = await c.get(CF_ROOT, headers=_h(
            "https://httpbin.org", "/response-headers?status=429&Retry-After=7"))
        # httpbin /response-headers selalu 200 dengan header yang diminta;
        # yang penting relay meneruskan headernya apa adanya.
        assert r.headers.get("Retry-After") == "7", dict(r.headers)


@case("CF-E5 header platform: cloudflare (cf-ray), tanpa x-vercel-error, CORS *")
async def _e5():
    async with _client() as c:
        r = await c.get(CF_ROOT, headers=_h("https://httpbin.org", "/get"))
        assert r.status_code == 200, r.status_code
        assert r.headers.get("server") == "cloudflare", dict(r.headers)
        assert r.headers.get("cf-ray"), "tanpa cf-ray"
        assert "x-vercel-error" not in {k.lower() for k in r.headers}, \
            "ada penanda vercel di relay CF!"
        assert r.headers.get("access-control-allow-origin") == "*", \
            dict(r.headers)


@case("CF-E6 host invalid -> error non-200 terkontrol (bukan crash/hang)")
async def _e6():
    async with _client(timeout=40.0) as c:
        r = await c.get(CF_ROOT, headers=_h("https://nonexistent.invalid", "/"))
        assert r.status_code != 200, r.status_code
        print(f"      [status={r.status_code}]")


@case("CF-E7 IP via relay != IP direct (masking egress)")
async def _e7():
    async with _client() as c:
        direct = (await c.get("https://api.ipify.org?format=json")).json()["ip"]
        via = (await c.get(CF_ROOT, headers=_h("https://api.ipify.org",
                                               "/?format=json"))).json()["ip"]
    assert via and direct and via != direct, f"direct={direct} via={via}"
    print(f"      [direct={direct} via={via}]")


# ---------------- G: kompatibilitas gateway ----------------

@case("CF-G1 header builder gateway (_with_relay_headers) diterima worker CF")
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


@case("CF-G2 header stream gateway (Accept SSE + identity) picu framing CF")
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
            lines, raw, _ = await _collect_stream(r, t0)
    _assert_sse_wrapped(lines, raw, "G2")


@case("CF-G3 _normalize_relay_url terima host CF -> /api/relay yang hidup")
async def _g3():
    from app.core.config import _normalize_relay_url
    norm = _normalize_relay_url("relay.sinug.workers.dev")
    assert norm == "https://relay.sinug.workers.dev/api/relay", norm
    async with _client() as c:
        r = await c.post(norm, headers=_h("https://httpbin.org", "/post"),
                         json={"norm": True})
        assert r.status_code == 200, (r.status_code, r.text[:200])
        assert r.json()["json"] == {"norm": True}


async def _run_one(name, fn):
    t0 = time.time()
    try:
        await asyncio.wait_for(fn(), timeout=280.0)
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


async def main():
    print(f"cf-relay suite: {len(_results)} case, SEMUA real HTTP ke {CF_ROOT}")
    print("=" * 70)
    passed = failed = 0
    for name, fn in _results:
        ok = await _run_one(name, fn)
        passed, failed = passed + ok, failed + (not ok)
        await asyncio.sleep(1)  # sopan ke worker + httpbin
    print("=" * 70)
    print(f"hasil: {passed} passed, {failed} failed dari {len(_results)} case")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

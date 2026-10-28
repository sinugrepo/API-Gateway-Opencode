"""Validasi rate guard (admission control + atribusi 429) — harus ALL PASSED.

Jalankan dari repo root:  python test/test_rate_guard.py
Keluar dengan kode 0 bila semua passed, 1 bila ada yang gagal.

Latar: 429 pada gateway ini punya dua penyebab yang sangat berbeda —
(a) laju request kita sendiri melebihi kuota, dan (b) kuota upstream memang
habis. Keduanya terlihat sama di klien, jadi tanpa admission control dan
pencatatan pola, satu-satunya respons yang mungkin adalah "tambah rotasi",
yang justru memperburuk (a).

Cakupan:
  A. Token bucket: menahan laju, burst, dan mode nonaktif
  B. Backoff: Retry-After, full jitter, plafon
  C. Budget rotasi 429
  D. Klasifikasi pola: target / sesi / akun / platform relay
  E. Snapshot dashboard + wiring ke jalur streaming
"""
import asyncio
import os
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS, FAIL = "PASS", "FAIL"
_results = []


def case(name):
    def deco(fn):
        _results.append((name, fn))
        return fn
    return deco


def _fresh_bucket(rate, burst):
    from app.services import rate_guard as rg

    rg.reset()
    rg._bucket = rg._TokenBucket(rate, burst)
    return rg


# ---------- A. token bucket ----------

@case("A1 bucket menahan laju: burst=1, rate=20/s -> 3 token butuh ~0.1s")
def _a1():
    rg = _fresh_bucket(20.0, 1)

    async def run():
        started = time.monotonic()
        for _ in range(3):
            await rg.upstream_token()
        return time.monotonic() - started

    elapsed = asyncio.run(run())
    assert elapsed >= 0.06, f"terlalu cepat ({elapsed:.3f}s), throttle tidak jalan"


@case("A2 burst mengizinkan N request awal tanpa menunggu")
def _a2():
    rg = _fresh_bucket(1.0, 3)

    async def run():
        started = time.monotonic()
        for _ in range(3):
            await rg.upstream_token()
        return time.monotonic() - started

    elapsed = asyncio.run(run())
    assert elapsed < 0.05, f"burst tidak dihormati ({elapsed:.3f}s)"


@case("A3 rate<=0 = tanpa throttle (mode darurat)")
def _a3():
    rg = _fresh_bucket(0.0, 1)

    async def run():
        started = time.monotonic()
        for _ in range(5):
            await rg.upstream_token()
        return time.monotonic() - started

    elapsed = asyncio.run(run())
    assert elapsed < 0.05, f"rate=0 masih menunggu ({elapsed:.3f}s)"


@case("A4 token bucket memulihkan token seiring waktu (tidak terkunci)")
def _a4():
    rg = _fresh_bucket(50.0, 1)

    async def run():
        await rg.upstream_token()
        await asyncio.sleep(0.1)  # 50/s * 0.1s = 5 token
        started = time.monotonic()
        await rg.upstream_token()
        return time.monotonic() - started

    elapsed = asyncio.run(run())
    assert elapsed < 0.03, f"token tidak pulih setelah idle ({elapsed:.3f}s)"


# ---------- B. backoff ----------

@case("B1 Retry-After upstream dihormati apa adanya")
def _b1():
    from app.services import rate_guard as rg

    delay = rg.backoff_delay(1, 7.5)
    assert abs(delay - 7.5) < 1e-6, f"Retry-After diabaikan: {delay}"


@case("B2 Retry-After di atas plafon dipotong (tidak menggantung selamanya)")
def _b2():
    from app.services import rate_guard as rg

    delay = rg.backoff_delay(1, 10_000)
    assert delay <= rg.UPSTREAM_BACKOFF_MAX + 1e-6, f"tidak dipotong: {delay}"


@case("B3 tanpa Retry-After: full jitter di dalam plafon eksponensial")
def _b3():
    from app.services import rate_guard as rg

    base = float(rg.RATE_LIMIT_BACKOFF)
    samples = [rg.backoff_delay(1) for _ in range(200)]
    assert all(0 <= s <= base + 1e-6 for s in samples), "jitter keluar batas attempt-1"
    assert len(set(samples)) > 1, "tidak acak (semua nilai identik) -> retry serentak"
    # attempt tinggi tetap dibatasi plafon
    high = [rg.backoff_delay(30) for _ in range(50)]
    assert all(h <= rg.UPSTREAM_BACKOFF_MAX + 1e-6 for h in high), "plafon bocor"


@case("B4 attempt lebih tinggi -> rata-rata jeda lebih besar")
def _b4():
    from app.services import rate_guard as rg

    low = sum(rg.backoff_delay(1) for _ in range(400)) / 400
    high = sum(rg.backoff_delay(5) for _ in range(400)) / 400
    assert high > low, f"backoff tidak tumbuh (low={low:.2f} high={high:.2f})"


# ---------- C. budget rotasi ----------

@case("C1 budget rotasi 429 dihormati")
def _c1():
    from app.services import rate_guard as rg

    budget = rg.UPSTREAM_MAX_429_ROTATIONS
    assert budget > 0, "budget default harus > 0 agar amplifikasi berhenti"
    assert not rg.rotations_exhausted(budget - 1), "berhenti terlalu awal"
    assert rg.rotations_exhausted(budget), "budget tidak menahan rotasi"


@case("C2 budget 0 = perilaku lama (tanpa batas)")
def _c2():
    from app.services import rate_guard as rg

    original = rg.UPSTREAM_MAX_429_ROTATIONS
    try:
        rg.UPSTREAM_MAX_429_ROTATIONS = 0
        assert not rg.rotations_exhausted(999), "budget 0 seharusnya tanpa batas"
    finally:
        rg.UPSTREAM_MAX_429_ROTATIONS = original


# ---------- D. klasifikasi ----------

def _record(rg, n, *, target, session, source="upstream"):
    for i in range(n):
        rg.record_429(
            route="RELAY",
            target=target(i) if callable(target) else target,
            session=session(i) if callable(session) else session,
            model="m1",
            attempt=1,
            source=source,
            retry_after=2.0,
        )


@case("D1 tanpa event -> verdict none")
def _d1():
    rg = _fresh_bucket(6.0, 6)
    verdict = rg.classify()
    assert verdict["verdict"] == "none", verdict
    assert verdict["samples"] == 0


@case("D2 satu target, banyak sesi -> target-scoped (limit per-IP)")
def _d2():
    rg = _fresh_bucket(6.0, 6)
    _record(rg, 5, target="https://relay-a.example/api/relay", session=lambda i: f"ses_{i}")
    verdict = rg.classify()
    assert verdict["verdict"] == "target-scoped", verdict
    assert verdict["evidence"]["distinct_targets"] == 1


@case("D3 satu sesi, banyak target -> session-scoped (limit per-sesi)")
def _d3():
    rg = _fresh_bucket(6.0, 6)
    _record(rg, 4, target=lambda i: f"https://relay-{i}.example/api/relay", session="ses_sama")
    verdict = rg.classify()
    assert verdict["verdict"] == "session-scoped", verdict
    assert verdict["evidence"]["distinct_sessions"] == 1


@case("D4 banyak target + banyak sesi -> account-scoped (kuota akun)")
def _d4():
    rg = _fresh_bucket(6.0, 6)
    _record(rg, 5, target=lambda i: f"https://relay-{i}.example/api/relay", session=lambda i: f"ses_{i}")
    verdict = rg.classify()
    assert verdict["verdict"] == "account-scoped", verdict
    assert verdict["evidence"]["distinct_targets"] >= 3
    assert verdict["evidence"]["distinct_sessions"] >= 2


@case("D5 mayoritas sumber vercel -> relay-platform (bukan OpenCode)")
def _d5():
    rg = _fresh_bucket(6.0, 6)
    _record(rg, 5, target=lambda i: f"https://relay-{i}.example/api/relay", session=lambda i: f"ses_{i}", source="vercel")
    verdict = rg.classify()
    assert verdict["verdict"] == "relay-platform", verdict


@case("D6 jendela waktu: event lama tidak ikut diklasifikasi")
def _d6():
    rg = _fresh_bucket(6.0, 6)
    _record(rg, 5, target="https://relay-a.example/api/relay", session="ses_x")
    assert rg.classify()["verdict"] == "target-scoped"
    # jendela 0 detik -> semua event di luar jendela
    assert rg.classify(window=-1)["verdict"] == "none"


# ---------- E. snapshot + wiring ----------

@case("E1 snapshot punya admission, klasifikasi, dan recent terbaru dulu")
def _e1():
    rg = _fresh_bucket(6.0, 6)
    _record(rg, 2, target="https://relay-a.example/api/relay", session="ses_x")
    snap = rg.snapshot()
    for key in ("admission", "classification", "recent", "rate_limited_pct", "by_target", "by_route"):
        assert key in snap, f"snapshot kehilangan {key}"
    assert snap["rate_limited"] == 2
    assert len(snap["recent"]) == 2
    assert snap["recent"][0]["ts"] >= snap["recent"][-1]["ts"], "recent tidak terurut terbaru dulu"
    assert snap["admission"]["max_rps"] == 6.0


@case("E2 host target dinormalisasi (path tidak memecah hitungan)")
def _e2():
    from app.services import rate_guard as rg

    assert rg._host_of("https://relay-a.example/api/relay") == "relay-a.example"
    assert rg._host_of("https://relay-a.example") == "relay-a.example"
    assert rg._host_of("") == "-"


@case("E3 jalur streaming + responses memakai gate dan budget")
def _e3():
    import inspect

    import app.services.responses_bridge as rb
    import app.services.streaming as st

    st_src = inspect.getsource(st)
    rb_src = inspect.getsource(rb)
    assert "await upstream_token()" in st_src, "streaming.py tidak memanggil gate"
    assert "record_429(" in st_src, "streaming.py tidak mencatat 429"
    assert "rotations_exhausted(" in st_src, "streaming.py tidak memakai budget"
    assert "await upstream_token()" in rb_src, "responses_bridge.py tidak memanggil gate"
    assert "record_429(" in rb_src, "responses_bridge.py tidak mencatat 429"


@case("E4 default konfigurasi waras (throttle aktif, budget terbatas)")
def _e4():
    from app.services import rate_guard as rg

    assert rg.UPSTREAM_MAX_RPS > 0, "RPS default 0 = throttle mati tanpa disadari"
    assert rg.UPSTREAM_BURST >= 1
    assert 1 <= rg.UPSTREAM_MAX_429_ROTATIONS <= 8, "budget harus kecil agar tidak amplifikasi"
    assert rg.UPSTREAM_BACKOFF_MAX >= float(rg.RATE_LIMIT_BACKOFF)
    assert rg.UPSTREAM_429_WINDOW > 0


@case("E5 reset mengosongkan recorder tanpa menyentuh konfigurasi")
def _e5():
    rg = _fresh_bucket(6.0, 6)
    _record(rg, 3, target="https://relay-a.example/api/relay", session="ses_x")
    out = rg.reset()
    assert out["events_cleared"] == 3
    snap = rg.snapshot()
    assert snap["rate_limited"] == 0
    assert snap["classification"]["verdict"] == "none"
    assert snap["admission"]["max_rps"] == 6.0, "reset tidak boleh mengubah konfigurasi"


def main():
    failed = 0
    for name, fn in _results:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"{FAIL}  {name}\n      {type(exc).__name__}: {exc}")
            traceback.print_exc(limit=2)
        else:
            print(f"{PASS}  {name}")
    print(f"\n{len(_results) - failed}/{len(_results)} passed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

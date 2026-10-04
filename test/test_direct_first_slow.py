"""Validasi kebijakan relay-first (harus ALL PASSED).

Jalankan dari repo root:  python test/test_direct_first_slow.py
Keluar dengan kode 0 bila semua passed, 1 bila ada yang gagal.

Kebijakan (operator): non-vision WAJIB relay dulu, direct hanya fallback
terakhir setelah SEMUA relay gagal. Vision tetap direct-first (biner base64
rawan 413/504 relay). `DIRECT_FIRST_SLOW` deprecated (no-op, dibaca agar env
lama tidak crash tapi tidak mengubah urutan target).

Cakupan (semua offline, tanpa network):
  A. Helper reorder tetap ada & benar (dipakai test lain / kompatibilitas)
  B. Deteksi model/giant tetap ada (dipakai guard giant-payload upstream)
  C. Ketiga generator TIDAK lagi direct-first slow, vision guard tetap utuh
  D. Knob config tetap ada (kompat env lama) + default false
"""
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


def _relay(url):
    return (url, {"x-relay-target": "https://opencode.ai", "x-relay-path": "/zen"})


def _direct():
    return ("https://opencode.ai/zen/v1/responses", {"Authorization": "Bearer x"})


# ---------- A. helper reorder (kompatibilitas) ----------

@case("A1 direct ke depan, relay utuh sebagai fallback")
def _a1():
    from app.services.relay import _order_targets_direct_first
    out = _order_targets_direct_first([_relay("r1"), _relay("r2"), _direct()])
    assert out[0][0].startswith("https://opencode.ai"), out
    assert [u for u, _ in out[1:]] == ["r1", "r2"], out
    assert len(out) == 3, "tidak ada target dibuang"


@case("A2 idempoten bila direct sudah di depan; tanpa direct tetap")
def _a2():
    from app.services.relay import _order_targets_direct_first
    only_direct = [_direct()]
    assert _order_targets_direct_first(only_direct) == only_direct
    relays = [_relay("r1"), _relay("r2")]
    assert _order_targets_direct_first(relays) == relays
    mixed = [_direct(), _relay("r1")]
    assert _order_targets_direct_first(mixed) == mixed


# ---------- B. deteksi model/giant (dipakai guard lain) ----------

@case("B1 spark terdeteksi responses-only; mimo/gpt tidak")
def _b1():
    from app.core.config import _is_responses_only_model
    assert _is_responses_only_model("muse-spark-1.3-contributor-free") is True
    assert _is_responses_only_model("gpt-5") is False
    assert _is_responses_only_model("mimo-v2.6-flash-free") is False


@case("B2 payload raksasa terdeteksi; kecil tidak")
def _b2():
    from app.services.relay import _is_giant_payload
    small = {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
    assert _is_giant_payload(small) is False
    many = {"model": "m",
            "messages": [{"role": "user", "content": "x"} for _ in range(101)]}
    assert _is_giant_payload(many) is True


# ---------- C. generator: relay-first enforced ----------

@case("C1 ketiga generator TANPA direct-first slow")
def _c1():
    import inspect
    import app.services.streaming as s
    import app.services.responses_bridge as b
    for fn in (s.stream_generator,
               b.responses_stream_generator,
               b.responses_to_chat_stream_generator):
        src = inspect.getsource(fn)
        assert "DIRECT_FIRST_SLOW" not in src, fn.__name__
        assert "_order_targets_direct_first" not in src, fn.__name__


@case("C2 vision guard + limit relay tetap utuh di ketiga generator")
def _c2():
    import inspect
    import app.services.streaming as s
    import app.services.responses_bridge as b
    for fn in (s.stream_generator,
               b.responses_stream_generator,
               b.responses_to_chat_stream_generator):
        src = inspect.getsource(fn)
        # Vision tetap direct-first, relay hanya fallback bila direct 429.
        assert "vision_direct_first and is_relay and not last_rate_limited" in src, \
            fn.__name__
        # Batas 2 relay + direct sebagai fallback terakhir dipertahankan.
        assert "_limit_stream_targets(targets)" in src, fn.__name__
        # Direct dibangun PALING AKHIR (fallback), bukan di depan.
        assert "targets.append((OPENCODE" in src, fn.__name__


# ---------- D. knob (kompat env lama) ----------

@case("D1 knob tetap ada + default false (no-op)")
def _d1():
    import app.core.config as c
    assert hasattr(c, "DIRECT_FIRST_SLOW")
    assert c.DIRECT_FIRST_SLOW is False


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

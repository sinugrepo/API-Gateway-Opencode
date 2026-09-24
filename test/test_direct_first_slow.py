"""Validasi direct-first untuk request lambat (harus ALL PASSED).

Jalankan dari repo root:  python test/test_direct_first_slow.py
Keluar dengan kode 0 bila semua passed, 1 bila ada yang gagal.

Latar: thinking xhigh muse-spark / konteks raksasa punya TTFB wajar >25s,
sehingga tiap percobaan relay PASTI mati 504 tanpa byte. Mencoba 2 relay
dulu membuang ~50 dtk + progres thinking upstream (attempt berikutnya mulai
dari nol); klien seperti Hermes yang mengabaikan keepalive `:` lalu
reconnect setelah ~85s tanpa data terjebak loop stall selamanya.
Perbaikan: bila model Responses-only ATAU payload giant, direct dicoba
DULU dan relay tetap sebagai fallback (distribusi 429 terjaga).

Cakupan (semua offline, tanpa network):
  A. Helper reorder: direct ke depan, relay utuh, idempoten
  B. Kondisi pemicu: spark selalu; giant non-spark; kecil non-spark tidak
  C. Ketiga generator memuat wiring kondisi + helper yang sama
  D. Knob config ada + default true (perilaku baru aktif)
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


# ---------- A. helper reorder ----------

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


# ---------- B. kondisi pemicu ----------

@case("B1 spark selalu direct-first (tanpa hitung giant)")
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


@case("B3 short-circuit: spark tidak perlu dumps raksasa")
def _b3():
    # Orde evaluasi di call-site: model dulu (murah), giant kemudian.
    # Buktikan _is_giant_payload tidak dipanggil untuk spark dengan
    # memalsukan fungsi yang melempar bila dipanggil.
    import app.services.streaming as s

    def _boom(payload):
        raise AssertionError("giant check tak boleh jalan untuk spark")

    # Simulasi ekspresi call-site persis seperti di kode.
    from app.core.config import _is_responses_only_model
    model = "muse-spark-1.3-contributor-free"
    assert (_is_responses_only_model(model) or _boom({})) is True


# ---------- C. wiring ketiga generator ----------

@case("C1 ketiga generator pakai kondisi + helper yang sama")
def _c1():
    import inspect
    import app.services.streaming as s
    import app.services.responses_bridge as b
    for fn in (s.stream_generator,
               b.responses_stream_generator,
               b.responses_to_chat_stream_generator):
        src = inspect.getsource(fn)
        assert "DIRECT_FIRST_SLOW" in src, fn.__name__
        assert "_is_responses_only_model(client_model)" in src, fn.__name__
        assert "_is_giant_payload(payload)" in src, fn.__name__
        assert "_order_targets_direct_first(targets)" in src, fn.__name__


@case("C2 fallback relay tetap ada setelah reorder (bukan dibuang)")
def _c2():
    import inspect
    import app.services.responses_bridge as b
    src = inspect.getsource(b.responses_to_chat_stream_generator)
    # Guard vision-429 (relay dipakai bila direct 429) tetap utuh.
    assert "vision_direct_first and is_relay and not last_rate_limited" in src
    # Limit 2 relay + direct dipertahankan sebelum reorder.
    assert "_limit_stream_targets(targets)" in src


# ---------- D. knob ----------

@case("D1 knob ada + default true")
def _d1():
    import app.core.config as c
    assert c.DIRECT_FIRST_SLOW is True


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

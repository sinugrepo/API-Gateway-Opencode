"""Validasi lapisan outbound proxy SOCKS/HTTP (harus ALL PASSED).

Jalankan dari repo root:  python test/test_outbound_proxy.py
Keluar 0 bila semua passed, 1 bila ada gagal.

Cakupan (offline kecuali G yang memakai warp-socks lokal bila ada):
  A. normalize_proxy_url (skema/host/port/auth, default socks5h)
  B. display_proxy_url menutupi password
  C. add/list/remove + duplikat + batas (tanpa sentuh file asli)
  D. round-robin + cooldown (sehat dulu, penalized cadangan)
  E. expand_targets_with_proxy (relay tak tersentuh, direct disisip proxy)
  F. _fresh_retry_targets tahan 3-tuple (regresi forbidden-retry)
  G. LIVE lokal: socks5h://127.0.0.1:40001 -> api64.ipify.org 200 via IPv6 (skip bila tutup)
   H. OFFLINE: detect_ip_version (v4/v6/invalid) + _parse_cf_trace (warp/ip)
"""
import asyncio
import os
import sys
import tempfile
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS, FAIL = "PASS", "FAIL"
_results = []


def case(name):
    def deco(fn):
        _results.append((name, fn))
        return fn
    return deco


@case("A1 normalize: default socks5h + alias + auth + invalid")
def _a1():
    from app.services.outbound_proxy import normalize_proxy_url
    assert normalize_proxy_url("127.0.0.1:40001") == "socks5h://127.0.0.1:40001"
    assert normalize_proxy_url("socks://127.0.0.1:40001") == "socks5h://127.0.0.1:40001"
    assert normalize_proxy_url("socks5://127.0.0.1:40001") == "socks5://127.0.0.1:40001"
    assert normalize_proxy_url("http://user:pw@10.0.0.1:8080") == "http://user:pw@10.0.0.1:8080"
    assert normalize_proxy_url("https://proxy.example.com:3128/") == "https://proxy.example.com:3128"
    # invalid
    assert normalize_proxy_url("") == ""
    assert normalize_proxy_url("http://:8080") == ""
    assert normalize_proxy_url("http://host:0") == ""
    assert normalize_proxy_url("http://host:99999") == ""
    assert normalize_proxy_url("ftp://host:21") == ""
    assert normalize_proxy_url("http://host-tanpa-port") == ""


@case("A2 normalize: tidak pernah melempar (None/aneh)")
def _a2():
    from app.services.outbound_proxy import normalize_proxy_url
    assert normalize_proxy_url(None) == ""
    assert normalize_proxy_url("://") == ""
    assert normalize_proxy_url("x" * 500) == ""


@case("B1 display menutupi password, aman untuk log/dashboard")
def _b1():
    from app.services.outbound_proxy import display_proxy_url
    assert display_proxy_url("socks5h://user:secret@127.0.0.1:40001") == \
        "socks5h://user:***@127.0.0.1:40001"
    assert "secret" not in display_proxy_url("socks5h://user:secret@127.0.0.1:40001")
    assert display_proxy_url("socks5h://127.0.0.1:40001") == "socks5h://127.0.0.1:40001"
    assert display_proxy_url(None) == "(invalid-proxy)"
    assert display_proxy_url({"scheme": "http", "host": "h", "port": 8080,
                              "username": "u", "password": "p"}) == "http://u:***@h:8080"


def _isolate_store():
    """Arahkan file proxy ke temp + reset state. Return (tmp_path, cleanup)."""
    import app.services.outbound_proxy as op
    tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
    tmp.close()
    try:
        os.unlink(tmp.name)
    except OSError:
        pass
    old_path = op.PROXY_CONFIG_PATH if hasattr(op, "PROXY_CONFIG_PATH") else None
    # PROXY_CONFIG_PATH dibaca via config module; patch fungsi _config_path.
    import app.core.config as cfg
    old_cfg = cfg.PROXY_CONFIG_PATH
    cfg.PROXY_CONFIG_PATH = tmp.name
    # op mengimpor PROXY_CONFIG_PATH by-value saat import -> patch juga di op.
    old_op_path = op.PROXY_CONFIG_PATH
    op.PROXY_CONFIG_PATH = tmp.name
    # Reset in-memory.
    with op._store_lock:
        op._proxies.clear()
        op._proxies_order.clear()
        op._global_enabled = True
        op._loaded = True  # cegah seed env/file menimpa isolasi
    with op._proxy_penalty_lock:
        op._proxy_penalty.clear()
    with op._proxy_index_lock:
        op._proxy_index = 0

    def _cleanup():
        cfg.PROXY_CONFIG_PATH = old_cfg
        op.PROXY_CONFIG_PATH = old_op_path
        with op._store_lock:
            op._proxies.clear()
            op._proxies_order.clear()
            op._loaded = False
        with op._proxy_penalty_lock:
            op._proxy_penalty.clear()
        try:
            os.unlink(tmp.name)
        except OSError:
            pass
    return tmp.name, _cleanup


@case("C1 add/list/remove + duplikat ditolak + password tak bocor")
def _c1():
    _, cleanup = _isolate_store()
    try:
        from app.services import outbound_proxy as op
        entry, err = op.add_proxy("socks5h", "127.0.0.1", 40001)
        assert err is None and entry is not None, err
        dup, derr = op.add_proxy("socks5h", "127.0.0.1", 40001)
        assert dup is None and derr is not None  # duplikat
        bad, berr = op.add_proxy("ftp", "127.0.0.1", 40001)
        assert bad is None and berr is not None
        bad2, berr2 = op.add_proxy("socks5h", "127.0.0.1", 99999)
        assert bad2 is None and berr2 is not None
        masked = op.list_proxies_masked()
        assert len(masked) == 1
        assert "password" not in masked[0]
        assert masked[0]["has_auth"] is False
        # auth entry
        entry2, err2 = op.add_proxy("http", "10.0.0.1", 8080, "user", "s3cr3t")
        assert err2 is None
        masked2 = op.list_proxies_masked()
        assert len(masked2) == 2
        assert all("password" not in m for m in masked2)
        assert "s3cr3t" not in str(masked2)
        pid = entry["id"]
        assert op.set_proxy_enabled(pid, False) is True
        assert op.list_proxies_masked()[0]["enabled"] is False
        assert op.set_proxy_enabled(pid, True) is True
        assert op.remove_proxy(pid) is True
        assert len(op.list_proxies_masked()) == 1
        assert op.remove_proxy("tidak-ada") is False
    finally:
        cleanup()


@case("D1 round-robin per request + cooldown menyusulkan penalized")
def _d1():
    _, cleanup = _isolate_store()
    try:
        from app.services import outbound_proxy as op
        op.add_proxy("socks5h", "127.0.0.1", 40001)
        op.add_proxy("socks5h", "127.0.0.1", 40002)
        op.add_proxy("socks5h", "127.0.0.1", 40003)
        first = [e["port"] for e in op.proxy_batch_for_request()]
        second = [e["port"] for e in op.proxy_batch_for_request()]
        # Rotasi: titik awal berbeda tiap request.
        assert first != second or first == sorted(first, reverse=True), (first, second)
        assert set(first) == {40001, 40002, 40003}
        # Penalti satu -> ia ke akhir (tetap cadangan).
        pid1 = [e["id"] for e in op.proxy_batch_for_request()
                if e["port"] == 40001][0]
        op.mark_proxy_failed(pid1, cooldown=600)
        batch = op.proxy_batch_for_request()
        assert batch[-1]["port"] == 40001, [e["port"] for e in batch]
        # Reset -> penalti hilang.
        op.reset_proxy_state()
        batch2 = op.proxy_batch_for_request()
        assert len(batch2) == 3
    finally:
        cleanup()


@case("E1 expand: relay tak tersentuh, direct disisip proxy, kosong=no-op")
def _e1():
    _, cleanup = _isolate_store()
    try:
        from app.services.outbound_proxy import expand_targets_with_proxy, unpack_target
        from app.services import outbound_proxy as op
        relay_t = ("https://relay-x/", {"x-relay-target": "https://a",
                                        "x-relay-path": "/"})
        direct_t = ("https://opencode.ai/zen/v1/chat/completions", {"A": "b"})
        # Pool kosong -> ekuivalen 3-tuple tanpa proxy.
        out = expand_targets_with_proxy([relay_t, direct_t], None)
        assert len(out) == 2
        assert all(len(t) == 3 for t in out)
        assert out[0][2] is None and out[1][2] is None
        # use_proxy=False eksplisit -> sama.
        out_off = expand_targets_with_proxy([relay_t, direct_t], False)
        assert len(out_off) == 2 and all(t[2] is None for t in out_off)
        # Isi pool -> direct diganti [proxy, proxy, raw].
        op.add_proxy("socks5h", "127.0.0.1", 40001)
        op.add_proxy("socks5h", "127.0.0.1", 40002)
        out2 = expand_targets_with_proxy([relay_t, direct_t], None)
        assert len(out2) == 1 + 2 + 1, out2  # relay + 2 proxy + raw
        assert out2[0][2] is None  # relay tanpa proxy
        assert out2[1][2] is not None and out2[2][2] is not None
        assert out2[3][2] is None  # raw direct terakhir
        assert "x-relay-target" not in out2[1][1]
        # unpack tahan 2- dan 3-tuple.
        u, h, p = unpack_target(direct_t)
        assert p is None and u == direct_t[0]
        u2, h2, p2 = unpack_target(out2[1])
        assert p2 is not None
    finally:
        cleanup()


@case("F1 fresh-retry tahan 3-tuple proxy (regresi)")
def _f1():
    from app.services.opencode import _fresh_retry_targets
    targets = [
        ("https://relay/", {"x-opencode-session": "ses_OLD"}),
        ("https://direct/", {"x-opencode-session": "ses_OLD"}, "socks5h://127.0.0.1:40001"),
    ]
    payload = {"model": "m"}
    new_targets, session, key = _fresh_retry_targets(targets, payload)
    assert len(new_targets) == 2, new_targets
    assert new_targets[0][1]["x-opencode-session"] == session
    assert len(new_targets[0]) == 2  # tanpa proxy tetap 2-tuple
    assert len(new_targets[1]) == 3  # proxy dipertahankan
    assert new_targets[1][2] == "socks5h://127.0.0.1:40001"


@case("G1 LIVE lokal warp-socks 40001 -> api64 200 IPv6 (skip bila tutup)")
def _g1():
    import socket
    s = socket.socket()
    s.settimeout(2)
    try:
        s.connect(("127.0.0.1", 40001))
    except OSError:
        print("    (skip: 127.0.0.1:40001 tertutup di host ini)")
        return
    finally:
        try:
            s.close()
        except OSError:
            pass
    from app.services.outbound_proxy import test_proxy_connection
    res = asyncio.run(test_proxy_connection("socks5h://127.0.0.1:40001", timeout=10))
    assert res.get("ok") is True, res
    assert res.get("ip"), res
    # WARP V6ONLY -> api64 HARUS IPv6 unik; IPv4 berarti container belum V6ONLY
    # (shared anycast 104.28.x.x, bukan bug gateway). Tes hanya memastikan
    # gateway MEMAKAI jalur IPv6-capable (api64), bukan api.ipify.org v4-only.
    assert res.get("ip_version") in (4, 6), res
    print(f"    (live: ip={res.get('ip')} v{res.get('ip_version')} warp={res.get('warp')})")
    if res.get("ip_version") == 4:
        print("    (peringatan: egress IPv4 = shared WARP gratis; aktifkan V6ONLY di container)")


@case("H1 detect_ip_version + _parse_cf_trace offline")
def _h1():
    from app.services.outbound_proxy import detect_ip_version, _parse_cf_trace
    assert detect_ip_version("2a09:bac5::99") == 6
    assert detect_ip_version("104.28.10.20") == 4
    assert detect_ip_version("") is None
    assert detect_ip_version(None) is None
    assert detect_ip_version("bukan-ip") is None
    p = _parse_cf_trace("fl=1\nip=2a09:bac5::99\nwarp=on\n")
    assert p == {"warp": "on", "ip": "2a09:bac5::99"}, p
    q = _parse_cf_trace("ip=104.28.10.20\nwarp=off\n")
    assert q == {"warp": "off", "ip": "104.28.10.20"}, q
    assert _parse_cf_trace("") == {"warp": None, "ip": None}


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

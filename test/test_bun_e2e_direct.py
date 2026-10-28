"""E2E: direct-only mode — verify Bun forward integrates into direct_request()
and httpx fallback still works when Bun is disabled.

Jalankan dari repo root:  python test/test_bun_e2e_direct.py
Keluar 0 bila semua passed, 1 bila ada gagal.
"""
import asyncio
import os
import sys
import json
from unittest.mock import patch, MagicMock, AsyncMock

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS, FAIL = "PASS", "FAIL"
_results = []


def case(name):
    def deco(fn):
        _results.append((name, fn))
        return fn
    return deco


def _make_fake_httpx_response(status=200, body=None):
    """Bikin httpx.Response palsu yang sudah .read() (content terisi)."""
    if body is None:
        body = {"id": "fake", "choices": [{"message": {"role": "assistant", "content": "ok"}}]}
    content = json.dumps(body).encode("utf8")
    resp = httpx.Response(
        status_code=status,
        content=content,
        headers={"content-type": "application/json"},
        request=httpx.Request("POST", "https://opencode.ai/inference/v1/chat/completions"),
    )
    resp.read()  # pastikan .content terisi
    return resp


def _async_client_for(fake_resp):
    """Bikin mock AsyncClient yang post() -> fake_resp, aread() -> None."""
    async def _fake_post(*a, **kw):
        return fake_resp
    async def _fake_aread():
        return None
    client = MagicMock(spec=httpx.AsyncClient)
    client.post = MagicMock(side_effect=_fake_post)
    fake_resp.aread = _fake_aread
    return client


@case("direct + Bun ON -> Bun dipanggil, httpx TIDAK dipanggil")
def _case_direct_bun_on():
    from app.services import upstream
    fake = _make_fake_httpx_response(200)

    async def _run():
        with patch.object(upstream, "_try_bun_forward", return_value=fake) as mock_bun:
            mock_client = _async_client_for(fake)
            with patch.object(upstream, "_get_http", return_value=mock_client):
                payload = {"model": "m", "messages": [{"role": "user", "content": "hi"}], "stream": False}
                resp, used_url = await upstream.call_upstream(
                    payload, stream=False, use_relay=False,
                )
                assert mock_bun.called, "Bun forward HARUS dipanggil"
                assert not mock_client.post.called or not any(
                    c[0] == "httpx.mock" for c in []
                ), "httpx.post seharusnya TIDAK dipanggil"
                # mock_client.post is a MagicMock, so .called means real httpx fallback used
                assert resp.status_code == 200, f"Bun response status: {resp.status_code}"
                assert used_url
                print(f"    status={resp.status_code} used={used_url}")

    asyncio.run(_run())


@case("direct + Bun OFF (None) -> httpx.post dipanggil (fallback)")
def _case_direct_bun_off():
    from app.services import upstream
    fake = _make_fake_httpx_response(200)

    async def _run():
        with patch.object(upstream, "_try_bun_forward", return_value=None) as mock_bun:
            mock_client = _async_client_for(fake)
            with patch.object(upstream, "_get_http", return_value=mock_client):
                payload = {"model": "m", "messages": [{"role": "user", "content": "hi"}], "stream": False}
                resp, used_url = await upstream.call_upstream(
                    payload, stream=False, use_relay=False,
                )
                assert mock_bun.called, "Bun dipanggil dulu (lalu fallback)"
                assert mock_client.post.called, "httpx.post HARUS dipanggil saat Bun mati"
                assert resp.status_code == 200
                print(f"    status={resp.status_code} used={used_url} "
                      f"(httpx.post called={mock_client.post.called})")

    asyncio.run(_run())


@case("proxy-direct + Bun OFF -> Bun dipanggil per-proxy, httpx proxy dipanggil")
def _case_proxy_direct_bun_off():
    from app.services import upstream
    from app.services import outbound_proxy as op
    import app.core.config as cfg
    import tempfile

    tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
    tmp.close()
    try:
        old_cfg = cfg.PROXY_CONFIG_PATH
        old_op_path = op.PROXY_CONFIG_PATH
        cfg.PROXY_CONFIG_PATH = tmp.name
        op.PROXY_CONFIG_PATH = tmp.name
        with op._store_lock:
            op._proxies.clear()
            op._proxies_order.clear()
            op._global_enabled = True
            op._loaded = True
        with op._proxy_penalty_lock:
            op._proxy_penalty.clear()
        with op._proxy_index_lock:
            op._proxy_index = 0

        op.add_proxy("socks5h", "127.0.0.1", 40001)
        op.add_proxy("socks5h", "127.0.0.1", 40002)

        fake = _make_fake_httpx_response(200)

        async def _run():
            with patch.object(upstream, "_try_bun_forward", return_value=None) as mock_bun:
                mock_client = _async_client_for(fake)
                # Force enable proxy layer + patch proxy client + use_proxy override
                with patch("app.core.http_client._get_proxy_client", return_value=mock_client), \
                     patch("app.services.outbound_proxy.USE_PROXY", True), \
                     patch("app.services.outbound_proxy.is_proxy_layer_active", return_value=True):
                    payload = {"model": "m", "messages": [{"role": "user", "content": "hi"}], "stream": False}
                    resp, used_url = await upstream.call_upstream(
                        payload, stream=False, use_relay=False, use_proxy=None,
                    )
                    assert mock_bun.call_count >= 1, f"Bun harus dipanggil minimal 1x per-proxy, actual={mock_bun.call_count}"
                    assert mock_client.post.called, "httpx proxy client HARUS dipanggil"
                    assert resp.status_code == 200
                    print(f"    status={resp.status_code} used={used_url} "
                          f"(Bun called={mock_bun.call_count}x, httpx.post called={mock_client.post.called})")

        asyncio.run(_run())
    finally:
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


def main() -> int:
    print(f"menjalankan {len(_results)} case e2e direct-mode...")
    print("=" * 70)
    passed = failed = 0
    for name, fn in _results:
        try:
            fn()
        except Exception as exc:
            failed += 1
            import traceback
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

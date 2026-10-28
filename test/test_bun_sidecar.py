"""Test offline: Bun sidecar tidak ada -> fallback httpx tetap jalan."""
import sys
import types
from unittest.mock import patch, MagicMock

import pytest

# --- Simulate: bun_egress/bun_proxy importable tapi forward() gagal -> None
# (perilaku "Bun tidak ada / mati": request path otomatis fallback httpx).
from app.core import bun_egress
from app.core.bun_proxy import forward as real_forward


def _none_forward(*args, **kwargs):
    """Simulate Bun sidecar mati: forward selalu None -> fallback httpx."""
    return None


def test_bun_forward_none_falls_back_to_httpx():
    """Bila bun_egress.forward() -> None, _try_bun_forward return None (fallback httpx)."""
    with patch("app.core.bun_egress.forward", side_effect=_none_forward):
        from app.services.upstream import _try_bun_forward

        result = _try_bun_forward(
            "https://opencode.ai/inference/v1/chat/completions",
            {"Content-Type": "application/json", "Authorization": "Bearer test"},
            {"model": "test-model", "messages": [{"role": "user", "content": "hi"}]},
            proxy_url=None,
        )
        assert result is None, "_try_bun_forward harus None bila Bun mati"


def test_bun_forward_with_proxy_falls_back_when_disabled():
    """BUN_SIDECAR_ENABLED=False + proxy_url -> _try_bun_forward None."""
    with patch("app.core.bun_egress.BUN_SIDECAR_ENABLED", False):
        from app.services.upstream import _try_bun_forward

        result = _try_bun_forward(
            "https://opencode.ai/inference/v1/chat/completions",
            {"Content-Type": "application/json", "Authorization": "Bearer test"},
            {"model": "test-model", "messages": [{"role": "user", "content": "hi"}]},
            proxy_url="socks5h://127.0.0.1:40001",
        )
        assert result is None


def test_bun_enabled_false_disables_forward():
    """BUN_SIDECAR_ENABLED=false (default) -> bun_egress.forward() -> None."""
    with patch("app.core.bun_egress.BUN_SIDECAR_ENABLED", False):
        result = bun_egress.forward(
            method="POST",
            target_url="https://example.com",
            headers={"Content-Type": "application/json"},
            body=b"{}",
            proxy_url=None,
            timeout_s=10.0,
        )
        assert result is None


def test_bun_direct_only_mode():
    """BUN_SIDECAR_DIRECT_ONLY=true + proxy_url diisi -> None (proxy tidak lewat Bun)."""
    with patch("app.core.bun_egress.BUN_SIDECAR_ENABLED", True), \
         patch("app.core.bun_egress.BUN_SIDECAR_DIRECT_ONLY", True), \
         patch("app.core.bun_egress._bun_forward") as mock_fwd:
        mock_fwd.return_value = None
        # proxy_url diisi + DIRECT_ONLY=True -> use_bun() False -> forward() None
        result = bun_egress.forward(
            method="POST",
            target_url="https://example.com",
            headers={},
            body=b"{}",
            proxy_url="socks5h://127.0.0.1:40001",
        )
        assert result is None


def test_bun_direct_mode_allows_direct():
    """BUN_SIDECAR_DIRECT_ONLY=true + proxy_url=None (direct) -> forward() dipanggil."""
    with patch("app.core.bun_egress.BUN_SIDECAR_ENABLED", True), \
         patch("app.core.bun_egress.BUN_SIDECAR_DIRECT_ONLY", True), \
         patch("app.core.bun_egress._bun_forward") as mock_fwd:
        mock_fwd.return_value = {"status": 200, "retry_after": 0.0, "headers": {}, "body": b"{}", "url": "https://x"}
        result = bun_egress.forward(
            method="POST",
            target_url="https://example.com",
            headers={},
            body=b"{}",
            proxy_url=None,  # direct
        )
        assert result is not None, "direct + DIRECT_ONLY=True harus lewat Bun"


def test_bun_response_converts_result():
    """bun_response() mengubah dict hasil forward() menjadi httpx.Response palsu."""
    import httpx
    from app.core.bun_proxy import bun_response

    fake_result = {
        "status": 429,
        "retry_after": 5.0,
        "headers": {"content-type": "application/json"},
        "body": b'{"error":"rate_limited"}',
        "url": "https://opencode.ai/inference/v1/chat/completions",
    }
    resp = bun_response(fake_result)
    assert resp is not None
    assert isinstance(resp, httpx.Response)
    assert resp.status_code == 429
    assert resp.headers.get("retry-after") == "5.0"
    assert b"rate_limited" in resp.content

    # None -> None (fallback)
    assert bun_response(None) is None


def test_handle_error_proxy_not_supported_deactivates():
    """ERROR proxy-not-supported-in-bun -> _mark_live(False) permanen."""
    from app.core import bun_proxy

    # Patch _mark_live untuk menangkap flag
    with patch.object(bun_proxy, "_mark_live") as mock_mark:
        bun_proxy.handle_error("proxy-not-supported-in-bun: TLS not available")
        mock_mark.assert_called_once_with(False)


def test_handle_error_transient_falls_back():
    """ERROR transien (mis. OSError) -> _mark_live TIDAK dipanggil (fallback sekali)."""
    from app.core import bun_proxy

    with patch.object(bun_proxy, "_mark_live") as mock_mark:
        bun_proxy.handle_error("OSError: connection reset by peer")
        mock_mark.assert_not_called()


def test_live_false_when_disabled():
    """BUN_SIDECAR_ENABLED=False -> live() selalu False."""
    with patch("app.core.bun_proxy.BUN_SIDECAR_ENABLED", False):
        from app.core.bun_proxy import live
        assert live() is False

"""Helper terpusat: forward request egress (direct/proxy) via Bun sidecar.

Dipakai di semua titik gateway yang melakukan POST upstream **non-relay**
(direct maupun via pool proxy WARP-localhost). Bila Bun sidecar hidup,
request di-forward ke sidecar -> keluar dari server ini dengan fingerprint
TLS Bun asli. Bila sidecar mati / tidak enabled / menjawab ERROR, helper
mengembalikan None dan pemanggil OTOMATIS fallback ke httpx/socksio
(perilaku lama 100%) — tidak ada crash.

Jalur RELAY VERCEL tidak memakai helper ini: TLS-nya ada di sisi Vercel,
tidak bisa di-fingerprint dari server kami.

BUN_SIDECAR_DIRECT_ONLY (default false):
  - false (default)  -> direct DAN proxy WARP sama-sama lewat Bun.
  - true             -> hanya direct lewat Bun; proxy tetap httpx.
"""
from typing import Dict, Optional

from app.core.bun_proxy import forward as _bun_forward
from app.core.config import BUN_SIDECAR_DIRECT_ONLY, BUN_SIDECAR_ENABLED


def use_bun(proxy_url: Optional[str]) -> bool:
    """True bila egress ini seharusnya lewat Bun sidecar.

    `proxy_url`: URL proxy (mis. "socks5h://127.0.0.1:40001") atau None
    untuk direct.
    """
    if not BUN_SIDECAR_ENABLED:
        return False
    if BUN_SIDECAR_DIRECT_ONLY:
        # Hanya direct yang lewat Bun; proxy (WARP) tetap httpx.
        return proxy_url is None
    # Default: direct + proxy WARP sama-sama lewat Bun.
    return True


def proxy_spec_for(proxy_url: Optional[str]) -> str:
    """Normalisasi `proxy_url` (dari pool WARP) menjadi spec yang dipaham
    sidecar ("direct" atau "socks5h://...")."""
    cleaned = (proxy_url or "").strip()
    return cleaned if cleaned else "direct"


def forward(
    method: str,
    target_url: str,
    headers: Dict[str, str],
    body: bytes,
    proxy_url: Optional[str] = None,
    timeout_s: float = 120.0,
) -> Optional[Dict[str, object]]:
    """Forward satu request non-stream ke upstream via Bun bila layak.

    Return:
      - dict {"status","retry_after","headers","body"} bila sukses via Bun.
      - None bila harus fallback httpx (Bun mati / tidak applicable / ERROR).
    """
    if not use_bun(proxy_url):
        return None
    spec = proxy_spec_for(proxy_url)
    try:
        return _bun_forward(
            method=method,
            target_url=target_url,
            headers=headers,
            body=body,
            proxy_spec=spec,
            timeout_s=timeout_s,
        )
    except Exception:  # noqa: BLE001 - request path WAJIB tidak crash
        return None

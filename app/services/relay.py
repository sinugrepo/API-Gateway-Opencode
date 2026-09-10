"""Relay round-robin, 429 cooldown, stream-broken marking."""
import asyncio
import json
import time
import threading
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx

from app.core.config import (
    API_KEY,
    OPENCODE_URL,
    RATE_LIMIT_BACKOFF,
    RATE_LIMIT_COOLDOWN,
    MAX_RELAY_STREAM_ATTEMPTS,
    RELAY_GIANT_PAYLOAD_BYTES,
    RELAY_STATUS_TIMEOUT,
    RELAY_STREAM_BROKEN_COOLDOWN,
    RELAY_URLS,
)
from app.core.logging_utils import _log
from app.core.http_client import _get_http


_relay_index = 0


_relay_index_lock = threading.Lock()


# Relays yang baru saja kena 429 masuk "cooldown". Selama cooldown, relay
# tersebut DILEWATI oleh rotasi (selama masih ada relay lain yang sehat),
# sehingga request berikutnya otomatis pindah ke relay yang berbeda dan relay
# yang "panas" punya waktu pulih. Ini mencegah request bertubi-tubi menabrak
# relay yang sama (penyebab utama munculnya 429 beruntun).
_relay_penalty: Dict[str, float] = {}


_relay_penalty_lock = threading.Lock()


def _mark_relay_rate_limited(url: str, until: float) -> None:
    """Tandai relay kena 429; ia tidak akan dipilih sampai `until`."""
    with _relay_penalty_lock:
        now = time.time()
        # Prune entri kedaluwarsa agar dict tidak tumbuh tanpa batas pada
        # proses long-running (stream berkali-kali dalam satu sesi).
        for key in [k for k, v in _relay_penalty.items() if v <= now]:
            del _relay_penalty[key]
        _relay_penalty[url] = until


def _is_relay_penalized(url: str) -> bool:
    with _relay_penalty_lock:
        return time.time() < _relay_penalty.get(url, 0.0)


# Relay yang stream-nya mati oleh timeout platform Vercel (504
# FUNCTION_INVOCATION_TIMEOUT) ditandai "stream-broken": fungsi serverless
# Vercel punya batas durasi eksekusi (~25 dtk terlihat di log), jadi ia TIDAK
# AKAN PERNAH bisa menahan stream panjang. Menandainya mencegah setiap
# request streaming berikutnya membuang ~25 dtk per relay (~75 dtk untuk
# 3 relay) sebelum akhirnya fallback ke direct. Non-streaming tetap boleh
# memakai relay ini (request pendek tidak kena timeout).
_relay_stream_broken: Dict[str, float] = {}


_relay_stream_broken_lock = threading.Lock()


def _is_giant_payload(payload: Any) -> bool:
    """True bila payload konteks raksasa (TTFB upstream wajar >25s).

    Heuristik murah: >100 item input/messages ATAU JSON > ambang byte.
    Request semacam ini DIBUNUH Edge (504) sebelum byte pertama walau relay
    sehat — kegagalan yang diharapkan, bukan cacat relay.
    """
    try:
        if isinstance(payload, dict):
            for key in ("input", "messages"):
                items = payload.get(key)
                if isinstance(items, list) and len(items) > 100:
                    return True
            return len(json.dumps(payload, ensure_ascii=False)) > RELAY_GIANT_PAYLOAD_BYTES
        return len(json.dumps(payload, ensure_ascii=False)) > RELAY_GIANT_PAYLOAD_BYTES
    except (TypeError, ValueError):
        return False


def _should_mark_stream_broken(url: str, payload: Any) -> bool:
    """Putuskan apakah 504 layak menandai relay stream-broken.

    False untuk konteks raksasa: menandainya meracuni relay sehat 30 menit
    (RELAY_STREAM_BROKEN_COOLDOWN) dan memaksa traffic normal ke direct.
    Pemanggil tetap merotasi ke target berikutnya; hanya penandaannya yang
    dilewati, dengan satu baris log sebagai jejak.
    """
    if _is_giant_payload(payload):
        try:
            size = len(json.dumps(payload, ensure_ascii=False))
        except (TypeError, ValueError):
            size = -1
        _log(
            "RELAY",
            f"giant-payload {url} | ~{size} byte: 504 Edge wajar "
            f"(TTFB>25s), relay TIDAK ditandai broken -> rotasi saja",
        )
        return False
    return True


def _limit_stream_targets(
    targets: List[Tuple[str, Dict[str, str]]],
) -> List[Tuple[str, Dict[str, str]]]:
    """Potong daftar kandidat streaming: max N relay + selalu sisakan direct.

    `targets` = [relay...] + [direct?]. Hanya porsi relay yang dipotong;
    target direct (tanpa x-relay-target) tidak pernah dibuang. Mencegah satu
    request streaming menggantung bermenit-menit menyapu semua relay yang
    sama-sama kena limit 25 dtk Vercel.
    """
    if MAX_RELAY_STREAM_ATTEMPTS < 0:
        return targets
    relays = [t for t in targets if "x-relay-target" in t[1]]
    directs = [t for t in targets if "x-relay-target" not in t[1]]
    if len(relays) > MAX_RELAY_STREAM_ATTEMPTS:
        _log(
            "RELAY",
            f"stream-limit {len(relays)} relay -> {MAX_RELAY_STREAM_ATTEMPTS} "
            f"percobaan + direct (hindari 25s x N hang Vercel)",
        )
        relays = relays[:MAX_RELAY_STREAM_ATTEMPTS]
    return relays + directs


def _mark_relay_stream_broken(url: str, until: float) -> None:
    """Tandai relay tak mampu streaming lama; di-skip sampai `until`."""
    with _relay_stream_broken_lock:
        now = time.time()
        for key in [k for k, v in _relay_stream_broken.items() if v <= now]:
            del _relay_stream_broken[key]
        _relay_stream_broken[url] = until


def _is_relay_stream_broken(url: str) -> bool:
    with _relay_stream_broken_lock:
        return time.time() < _relay_stream_broken.get(url, 0.0)


def _is_relay_timeout(response: httpx.Response) -> bool:
    """True bila response adalah timeout platform relay (bukan upstream).

    Hanya 504 + penanda Vercel yang dihitung. 500 dari upstream (mis.
    muse-spark via chat) diteruskan relay apa adanya dan TIDAK boleh
    menandai relay rusak — itu salah model/endpoint, bukan salah relay.
    """
    if response.status_code != 504:
        return False
    if response.headers.get("x-vercel-error"):
        return True
    body = (response.text or "")[:2000].lower()
    return any(
        marker in body
        for marker in (
            "function_invocation_timeout",
            "function_timeout",
            "invocation_timeout",
            "execution_timeout",
        )
    )


def _relay_batch_for_request(for_stream: bool = False) -> List[str]:
    """Susun daftar relay untuk SATU request, dengan titik awal dirotasi.

    Rotasi dilakukan PER REQUEST (bukan per target): tiap request memajukan
    index sebanyak 1, jadi request berikutnya SELALU mulai dari relay yang
    berbeda dari request sebelumnya — inilah yang membuat tiap request
    benar-benar ganti-ganti api vercel relay (bug lama: dengan jumlah relay
    genap, tiap request menghabiskan N index sehingga selalu mulai dari
    relay yang sama). Relay yang sedang cooldown 429 disusulkan ke akhir
    daftar (tetap dicoba sebagai cadangan kalau yang lain gagal).
    Bila `for_stream`, relay yang ditandai stream-broken (timeout Vercel
    pada stream panjang) juga disusulkan ke akhir agar request streaming
    langsung memakai jalur yang mampu (biasanya direct).
    """
    global _relay_index
    if not RELAY_URLS:
        return []
    num_relays = len(RELAY_URLS)
    with _relay_index_lock:
        start = _relay_index % num_relays
        _relay_index += 1
    urls = [RELAY_URLS[(start + i) % num_relays] for i in range(num_relays)]

    def _deprioritized(url: str) -> bool:
        if _is_relay_penalized(url):
            return True
        return for_stream and _is_relay_stream_broken(url)

    penalized = [u for u in urls if _deprioritized(u)]
    healthy = [u for u in urls if not _deprioritized(u)]
    if for_stream and penalized:
        _log(
            "RELAY",
            f"stream-skip {len(penalized)} relay tak-mampu-stream: "
            + ", ".join(u.split("//", 1)[-1].split("/", 1)[0] for u in penalized),
        )
    if for_stream and not healthy and penalized:
        # Semua relay tak-mampu-stream (bukan sekadar cooldown 429): mencoba
        # mereka hanya membuang ~25 dtk per relay sebelum 504. Drop total
        # agar streaming langsung ke direct. Kalau yang habis hanya karena
        # 429 (bisa pulih), tetap simpan sebagai cadangan.
        if all(_is_relay_stream_broken(u) for u in urls):
            _log("RELAY", "stream-skip SEMUA relay tak-mampu-stream -> langsung direct")
            return []
    return healthy + penalized


def _stream_request_headers(
    extra_headers: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """Build headers that preserve low-latency SSE streaming.

    Compression is intentionally disabled: a relay/CDN can buffer compressed
    response blocks, making token-level SSE arrive as large late bursts.
    `extra_headers` (identitas CLI dsb.) digabung apa adanya.
    """
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "text/event-stream",
        "Accept-Encoding": "identity",
        **(extra_headers or {}),
    }


def _relay_stream_headers(base_headers: Dict[str, str]) -> Dict[str, str]:
    """Build relay headers (x-relay-target/x-relay-path) for the SSE path."""
    return _with_relay_headers(OPENCODE_URL, base_headers)


def _with_relay_headers(target_url: str, base_headers: Dict[str, str]) -> Dict[str, str]:
    """Build relay headers for an arbitrary upstream target URL.

    Dipakai oleh path chat (/chat/completions) maupun Responses (/responses)
    agar keduanya bisa lewat rotasi relay yang sama.
    """
    parsed = urlparse(target_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    return {**base_headers, "x-relay-target": origin, "x-relay-path": path}


async def test_relay_connection(relay_url: str = RELAY_URLS[0]) -> Dict[str, Any]:
    """Cek semua relay SECARA PARALEL dengan timeout pendek.

    Perbaikan performance kritis: versi lama melakukan GET relay SATU PER SATU
    memakai shared client (read timeout 120s), sehingga /relay/status dan
    /monitor/api/relay bisa menggantung bermenit-menit dan memblokir refresh
    dashboard. Versi ini memakai asyncio.gather + timeout 8s per probe, jadi
    worst-case ~8s total untuk 14 relay, bukan 14x120s.
    """
    direct_ip = relay_ip = None
    start = time.time()

    async def _fetch_direct() -> Optional[str]:
        try:
            client = _get_http()
            direct = await asyncio.wait_for(
                client.get("https://api.ipify.org?format=json"),
                timeout=RELAY_STATUS_TIMEOUT,
            )
            data = direct.json()
            return data.get("ip") if isinstance(data, dict) else None
        except (httpx.TimeoutException, httpx.RequestError, asyncio.TimeoutError):
            return None
        except (ValueError, TypeError, AttributeError):
            return None

    async def _probe(url: str) -> Dict[str, Any]:
        try:
            headers = {
                "x-relay-target": "https://api.ipify.org",
                "x-relay-path": "/?format=json",
                "Accept": "application/json",
            }
            client = _get_http()
            response = await asyncio.wait_for(
                client.get(url, headers=headers),
                timeout=RELAY_STATUS_TIMEOUT,
            )
            if response.status_code == 200:
                try:
                    ip = response.json().get("ip")
                except (ValueError, TypeError, AttributeError):
                    ip = None
                return {"url": url, "ip": ip, "ok": ip is not None}
            return {"url": url, "ip": None, "ok": False}
        except (httpx.TimeoutException, httpx.RequestError, asyncio.TimeoutError):
            return {"url": url, "ip": None, "ok": False}
        except Exception:
            return {"url": url, "ip": None, "ok": False}

    direct_ip, *results = await asyncio.gather(
        _fetch_direct(), *(_probe(u) for u in RELAY_URLS)
    )
    results = list(results)

    relay_ip = next((r["ip"] for r in results if r["ok"]), None)

    elapsed = (time.time() - start) * 1000
    return {
        "success": relay_ip is not None,
        "relay_url": relay_url,
        "direct_ip": direct_ip,
        "relay_ip": relay_ip,
        "is_masked": relay_ip is not None and relay_ip != direct_ip,
        "response_time_ms": round(elapsed, 2),
        "error": None if relay_ip else "Gagal mendapatkan IP dari relay",
        "all_relays": results,
    }

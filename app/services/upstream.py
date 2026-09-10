"""Upstream payload + relay/direct call with 429 handling."""
import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
from fastapi import HTTPException
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_502_BAD_GATEWAY,
    HTTP_503_SERVICE_UNAVAILABLE,
)

from app.core.config import (
    API_KEY,
    HERMES_COMPAT,
    HERMES_TOOL_INSTRUCTION,
    MAX_RELAY_STREAM_ATTEMPTS,
    MODEL,
    OPENCODE_URL,
    RATE_LIMIT_BACKOFF,
    RATE_LIMIT_COOLDOWN,
    RATE_LIMIT_RETRIES,
    RELAY_FALLBACK,
    SPURIOUS_429_SAME_ROUTE_RETRIES,
    _is_responses_only_model,
)
from app.core.errors import TimeoutError_, UpstreamError
from app.core.logging_utils import _log
from app.services.opencode import _oc_session_tag
from app.services.relay import (
    _is_giant_payload,
    _is_relay_timeout,
    _mark_relay_rate_limited,
    _relay_batch_for_request,
    _stream_request_headers,
)
from app.core.schemas import ChatCompletionRequest
from app.core.http_client import _get_http


def _resolve_request_model(req: ChatCompletionRequest) -> str:
    """Resolve the client-selected model without inventing a provider default."""
    model = (req.model or MODEL).strip()
    if not model:
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail="The model field is required; choose one from GET /v1/models",
        )
    return model


def build_upstream_payload(req: ChatCompletionRequest) -> Dict[str, Any]:
    """Build a conservative OpenAI-compatible payload for the upstream API."""

    payload: Dict[str, Any] = {
        "model": _resolve_request_model(req),
        "messages": [message.to_upstream() for message in req.messages],
        "stream": req.stream,
    }

    # Forward only values explicitly supplied or safe defaults.
    for key in (
        "temperature",
        "max_tokens",
        "tools",
        "tool_choice",
        "parallel_tool_calls",
        "top_p",
        "stop",
        "presence_penalty",
        "frequency_penalty",
        "response_format",
        "seed",
        "n",
        "stream_options",
        "reasoning_effort",
    ):
        value = getattr(req, key, None)
        if value is not None:
            payload[key] = value

    # Some DeepSeek-compatible endpoints receive tool definitions but still
    # emit their DSML representation. The parser below converts it back to the
    # OpenAI structure Hermes expects.
    if HERMES_COMPAT and req.tools:
        payload["messages"] = [
            {"role": "system", "content": HERMES_TOOL_INSTRUCTION},
            *payload["messages"],
        ]

    # Always request usage info upstream on streaming so we can record tokens
    # even when the client did not set `stream_options.include_usage`. The
    # client-facing chunk-gating is handled in `chat_completions`.
    if req.stream:
        stream_opts = payload.get("stream_options")
        if not isinstance(stream_opts, dict):
            stream_opts = {}
        stream_opts["include_usage"] = True
        payload["stream_options"] = stream_opts

    return payload


def _retry_after_seconds(
    response: httpx.Response,
    fallback: float,
    max_seconds: float = 30.0,
) -> float:
    """Prefer the upstream `Retry-After` header over the fixed backoff.

    Only the common "Retry-After: <seconds>" form is honored (HTTP-date is
    ignored and falls back to the fixed backoff). The result is clamped to
    `max_seconds` so a misbehaving upstream cannot stall requests forever.
    """
    raw = response.headers.get("retry-after")
    if raw:
        try:
            parsed = float(raw)
        except (TypeError, ValueError):
            parsed = 0.0
        if 0 < parsed <= max_seconds:
            return parsed
    return fallback


def _classify_rate_limit(response: httpx.Response) -> Dict[str, str]:
    """Analisis ASAL-MUSUAL 429 yang diterima, untuk keterangan di log.

    Ada dua sumber umum 429 pada setup relay Vercel:

    1. RELAY VERCEL ITU SENDIRI (VERCEL):
       Vercel menolak sebelum/pada saat fungsi relay dijalankan — mis. batas
       invokasi fungsi harian plan Hobby (FUNCTION_INVOCATION_LIMIT) atau
       rate limit platform (TOO_MANY_REQUESTS). Tandanya: header
       `x-vercel-error`, atau body memuat "vercel" / "function_invocation".
       Jenis ini TIDAK sembuh dengan menunggu beberapa detik; relay harus
       di-cooldown dan request dipindahkan ke relay lain.

    2. UPSTREAM OPenCode (UPSTREAM):
       Relay hanya meneruskan 429 asli dari opencode.ai (rate limit API per
       IP publik relay / per akun). Tandanya: body JSON gaya OpenAI dengan
       `"error"` berisi "rate_limit" / "rate limit". Jenis ini bisa pulih
       setelah jeda Retry-After / backoff, lalu retry pada relay yang sama.
    """
    vercel_header = response.headers.get("x-vercel-error", "")
    body = (response.text or "")[:4000]
    body_lower = body.lower()

    vercel_markers = (
        "vercel",
        "function_invocation_limit",
        "function invocation",
        "too_many_requests",
    )
    upstream_markers = (
        "rate_limit",
        "rate limit",
        "ratelimit",
        "currently rate limited",
    )

    if vercel_header or any(m in body_lower for m in vercel_markers):
        return {
            "source": "vercel",
            "description": (
                "429 = RELAY VERCEL sendiri yang limit (invokasi fungsi harian / "
                "TOO_MANY_REQUESTS platform), BUKAN OpenCode. Menunggu sebentar "
                "tidak membantu; relay di-cooldown dan request dipindah ke relay lain."
            ),
            "hint": vercel_header or "x-vercel-error absent, body contains vercel marker",
        }
    if any(m in body_lower for m in upstream_markers):
        return {
            "source": "upstream",
            "description": (
                "429 diteruskan dari UPSTREAM OpenCode lewat relay (rate limit "
                "API opencode pada IP publik relay). Jenis ini bisa pulih: jeda "
                "Retry-After/backoff dulu, lalu retry target yang sama / relay lain."
            ),
            "hint": "opencode-style rate_limit error body",
        }
    return {
        "source": "unknown",
        "description": (
            "429 dengan body tak teridentifikasi. Baca header x-vercel-error dan "
            "isi body untuk membedakan: kalau ada kata 'vercel' -> relay Vercel "
            "yang limit; kalau error OpenAI 'rate_limit' -> OpenCode yang limit."
        ),
        "hint": body[:200],
    }


def _relay_cooldown_seconds(response: httpx.Response) -> float:
    """Durasi relay di-skip dari rotasi setelah kena 429.

    Lebih lama dari backoff sederhana (minimal RATE_LIMIT_COOLDOWN): rate
    limit upstream bersifat per-IP dan biasanya tidak pulih dalam hitungan
    detik, jadi relay yang "panas" harus dikesampingkan selama beberapa
    request berikutnya, baru boleh dicoba lagi setelah cooldown habis.
    """
    return max(
        _retry_after_seconds(response, RATE_LIMIT_BACKOFF * 2),
        RATE_LIMIT_COOLDOWN,
    )


def _raise_rate_limited(response: httpx.Response, message: str) -> None:
    """Convert an exhausted 429 into a clean, client-friendly UpstreamError.

    Raising a dedicated 429 (instead of folding into 502) lets the OpenAI-
    compatible client see `Retry-After` and wait, rather than retry-looping
    and spamming exceptions.
    """
    raise UpstreamError(
        message,
        status_code=HTTP_429_TOO_MANY_REQUESTS,
        upstream_status=429,
        retry_after=_retry_after_seconds(response, RATE_LIMIT_BACKOFF),
    )


def _should_retry_same_route_429(model: Any) -> bool:
    """True bila model kena bug spam-429 opencode -> retry 1x same-route dulu.

    Khusus model Responses-only (muse-spark & co.): 429 pertama pada satu
    route dicoba ulang sekali ke route YANG SAMA (dengan jeda backoff)
    sebelum relay di-cooldown / request dirotasi ke route berikutnya.
    Model lain selalu False (tetap langsung rotasi).
    """
    if SPURIOUS_429_SAME_ROUTE_RETRIES <= 0:
        return False
    try:
        return _is_responses_only_model(str(model or ""))
    except (TypeError, ValueError, AttributeError):
        return False


async def call_upstream(
    payload: Dict[str, Any],
    *,
    stream: bool,
    use_relay: bool,
    target_url: Optional[str] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Tuple[httpx.Response, str]:
    """Call upstream through round-robin relay, with optional direct fallback.

    `target_url` memilih endpoint upstream (default /chat/completions;
    Responses API memakai OPENCODE_RESPONSES_URL). Seluruh logika
    relay 429-cooldown + fallback direct dipakai ulang apa adanya.
    `extra_headers` (identitas CLI) dikirim baik direct maupun via relay.
    """

    if not API_KEY:
        raise UpstreamError(
            "OPENCODE_API_KEY is not configured",
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
        )

    upstream_url = target_url or OPENCODE_URL

    headers = (
        _stream_request_headers(extra_headers)
        if stream
        else {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {API_KEY}",
            "Accept": "application/json",
            **(extra_headers or {}),
        }
    )

    async def direct_request() -> Tuple[httpx.Response, str]:
        client = _get_http()
        r = await client.post(upstream_url, headers=headers, json=payload)
        await r.aread()
        return r, upstream_url

    async def relay_request(url: str) -> Tuple[httpx.Response, str]:
        parsed = urlparse(upstream_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        relay_headers = {
            "x-relay-target": base,
            "x-relay-path": path,
            **headers,
        }
        client = _get_http()
        r = await client.post(url, headers=relay_headers, json=payload)
        await r.aread()
        return r, url

    if not use_relay:
        try:
            resp, used = await direct_request()
        except httpx.TimeoutException as exc:
            raise TimeoutError_("Upstream request timed out") from exc
        except httpx.ConnectError as exc:
            raise UpstreamError(
                f"Cannot connect to upstream: {exc}",
                status_code=HTTP_503_SERVICE_UNAVAILABLE,
            ) from exc
        except httpx.RequestError as exc:
            raise UpstreamError(
                f"Request failed: {exc}",
                status_code=HTTP_502_BAD_GATEWAY,
            ) from exc

        # Direct-only: retry on 429 with backoff, then surface a clean 429.
        if resp.status_code == 429:
            _log(
                "RELAY",
                f"RATE-LIMITED DIRECT {upstream_url} | "
                f"{_classify_rate_limit(resp)['description']}",
            )
        for _retry in range(RATE_LIMIT_RETRIES):
            if resp.status_code != 429:
                break
            delay = _retry_after_seconds(resp, RATE_LIMIT_BACKOFF * (_retry + 1))
            _log(
                "RELAY",
                f"RATE-LIMITED DIRECT, retry {_retry + 1}/{RATE_LIMIT_RETRIES} "
                f"in {delay:.1f}s",
            )
            await asyncio.sleep(delay)
            try:
                resp, used = await direct_request()
            except httpx.TimeoutException as exc:
                raise TimeoutError_("Upstream request timed out") from exc
            except httpx.ConnectError as exc:
                raise UpstreamError(
                    f"Cannot connect to upstream: {exc}",
                    status_code=HTTP_503_SERVICE_UNAVAILABLE,
                ) from exc
            except httpx.RequestError as exc:
                raise UpstreamError(
                    f"Request failed: {exc}",
                    status_code=HTTP_502_BAD_GATEWAY,
                ) from exc

        if resp.status_code == 429:
            _raise_rate_limited(resp, "Upstream rate limited (429)")
        _log("RELAY", f"DIRECT {used}")
        return resp, used

    # Round-robin: try each relay URL in order. Batch dirotasi per request,
    # sehingga request berikutnya memulai dari relay yang BERBEDA.
    # Konteks raksasa: tiap relay Edge mati 25s (first-byte rule) sebelum
    # byte pertama keluar. Menyapu 11 relay = ~275s hang sebelum direct.
    # Batasi seperti streaming (0 = langsung direct).
    relay_batch = _relay_batch_for_request()
    if _is_giant_payload(payload) and MAX_RELAY_STREAM_ATTEMPTS >= 0:
        _log(
            "RELAY",
            f"giant-payload: batasi {len(relay_batch)} relay -> "
            f"{MAX_RELAY_STREAM_ATTEMPTS} percobaan + direct (hindari 25s x N)",
        )
        relay_batch = relay_batch[:MAX_RELAY_STREAM_ATTEMPTS]
    num_relays = len(relay_batch)
    for attempt in range(num_relays):
        url = relay_batch[attempt]
        _log("RELAY", f"ATTEMPT {attempt+1}/{num_relays} {url}")

        try:
            response, used_url = await relay_request(url)
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            if attempt == num_relays - 1 and not RELAY_FALLBACK:
                if isinstance(exc, httpx.TimeoutException):
                    raise TimeoutError_("All relays timed out") from exc
                raise UpstreamError(
                    f"All relays unavailable: {exc}",
                    status_code=HTTP_503_SERVICE_UNAVAILABLE,
                ) from exc
            _log("RELAY", f"FAIL {url} ({type(exc).__name__})")
            continue
        except httpx.RequestError as exc:
            if attempt == num_relays - 1:
                raise UpstreamError(
                    f"All relays failed: {exc}",
                    status_code=HTTP_502_BAD_GATEWAY,
                ) from exc
            _log("RELAY", f"FAIL {url} ({type(exc).__name__})")
            continue

        if response.status_code == 200:
            _log("RELAY", f"OK {url} | upstream-status={response.status_code}")
            return response, used_url

        # 429 pada IP relay: normalnya JANGAN retry pada IP yang sama
        # (hampir selalu 429 lagi). Catat relay masuk cooldown, lalu LANGSUNG
        # rotasi ke relay berikutnya. Kalau semua relay 429, fallback direct /
        # 429-bersih ditangani di bawah.
        # PENGECUALIAN bug spam-429 opencode khusus muse-spark
        # (Responses-only): retry 1x ke route YANG SAMA dulu sebelum rotasi.
        if response.status_code == 429:
            if _should_retry_same_route_429(payload.get("model")):
                for _same_retry in range(SPURIOUS_429_SAME_ROUTE_RETRIES):
                    delay = _retry_after_seconds(response, RATE_LIMIT_BACKOFF)
                    _log(
                        "RELAY",
                        f"SPURIOUS-429 {url} | retry same-route "
                        f"{_same_retry + 1}/{SPURIOUS_429_SAME_ROUTE_RETRIES} "
                        f"in {delay:.1f}s sebelum ganti route",
                    )
                    await asyncio.sleep(delay)
                    try:
                        _retry_resp, _retry_used = await relay_request(url)
                    except (httpx.TimeoutException, httpx.ConnectError, httpx.RequestError) as exc:
                        _log("RELAY", f"SPURIOUS-429 retry FAIL {url} ({type(exc).__name__})")
                        break
                    if _retry_resp.status_code == 200:
                        _log("RELAY", f"OK {url} | same-route retry sembuh (200)")
                        return _retry_resp, _retry_used
                    response = _retry_resp
                    if response.status_code != 429:
                        break
                    _log(
                        "RELAY",
                        f"SPURIOUS-429 {url} | masih 429 setelah same-route retry "
                        f"-> rotasi ke relay berikutnya",
                    )
            if response.status_code == 429:
                rate_cls = _classify_rate_limit(response)
                cooldown = _relay_cooldown_seconds(response)
                _mark_relay_rate_limited(url, time.time() + cooldown)
                _log(
                    "RELAY",
                    f"RATE-LIMITED {url} | {rate_cls['description']} | "
                    f"IP relay dirotasi terus -> relay berikutnya "
                    f"(cooldown {cooldown:.0f}s)",
                )

        _log("RELAY", f"FAIL {url} | upstream-status={response.status_code}")

        if attempt == num_relays - 1 and not RELAY_FALLBACK:
            if response.status_code == 429:
                _log(
                    "RELAY",
                    f"ALL RELAYS 429 | {_classify_rate_limit(response)['description']}",
                )
                _raise_rate_limited(
                    response,
                    f"All relays rate limited (429): {response.text[:200]}",
                )
            detail = response.text[:500]
            raise UpstreamError(
                f"All relays returned non-200. Last: {response.status_code}: {detail}",
                status_code=HTTP_502_BAD_GATEWAY,
                upstream_status=response.status_code,
            )

    # All relays failed, fall back to direct
    if RELAY_FALLBACK:
        _log("RELAY", f"FALLBACK all relays failed -> direct {upstream_url}")
        try:
            resp, used = await direct_request()
        except httpx.TimeoutException as exc:
            raise TimeoutError_("Direct fallback also timed out") from exc
        except httpx.ConnectError as exc:
            raise UpstreamError(
                f"Direct fallback failed: {exc}",
                status_code=HTTP_503_SERVICE_UNAVAILABLE,
            ) from exc
        except httpx.RequestError as exc:
            raise UpstreamError(
                f"Direct fallback failed: {exc}",
                status_code=HTTP_502_BAD_GATEWAY,
            ) from exc

        # Kalau direct fallback juga 429, retry dengan backoff lalu beri 429
        # yang bersih (bukan 502) agar klien bisa menunggu Retry-After.
        if resp.status_code == 429:
            _log(
                "RELAY",
                f"RATE-LIMITED DIRECT fallback | "
                f"{_classify_rate_limit(resp)['description']}",
            )
        for _retry in range(RATE_LIMIT_RETRIES):
            if resp.status_code != 429:
                break
            delay = _retry_after_seconds(resp, RATE_LIMIT_BACKOFF * (_retry + 1))
            _log(
                "RELAY",
                f"RATE-LIMITED DIRECT fallback, retry {_retry + 1}/"
                f"{RATE_LIMIT_RETRIES} in {delay:.1f}s",
            )
            await asyncio.sleep(delay)
            try:
                resp, used = await direct_request()
            except httpx.TimeoutException as exc:
                raise TimeoutError_("Direct fallback also timed out") from exc
            except httpx.ConnectError as exc:
                raise UpstreamError(
                    f"Direct fallback failed: {exc}",
                    status_code=HTTP_503_SERVICE_UNAVAILABLE,
                ) from exc
            except httpx.RequestError as exc:
                raise UpstreamError(
                    f"Direct fallback failed: {exc}",
                    status_code=HTTP_502_BAD_GATEWAY,
                ) from exc

        if resp.status_code == 429:
            _raise_rate_limited(resp, "Upstream rate limited (429)")
        _log("RELAY", f"FALLBACK OK {used}")
        return resp, used

    raise UpstreamError("No relay succeeded", status_code=HTTP_502_BAD_GATEWAY)

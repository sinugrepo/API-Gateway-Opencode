"""SSE streaming generator (chat completions)."""
import asyncio
import json
import secrets
import time
from contextlib import suppress
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import httpx
from fastapi import BackgroundTasks
from starlette.status import HTTP_503_SERVICE_UNAVAILABLE

from app.core.config import (
    API_KEY,
    BRIDGE_REQUEST_TIMEOUT,
    HERMES_COMPAT,
    OPENCODE_URL,
    RATE_LIMIT_BACKOFF,
    RATE_LIMIT_COOLDOWN,
    REASONING_FORWARD,
    RELAY_FALLBACK,
    RELAY_STREAM_BROKEN_COOLDOWN,
    REQUEST_TIMEOUT,
    SSE_KEEPALIVE_INTERVAL,
    STREAM_BYPASS_RELAY,
    USE_RELAY,
)
from app.core.errors import TimeoutError_, UpstreamEmptyResponse, UpstreamError
from app.core.http_client import _get_http
from app.core.logging_utils import _log
from app.services.opencode import _oc_session_tag
from app.core.sse import _sse
from app.services.relay import (
    _is_relay_penalized,
    _is_relay_stream_broken,
    _is_relay_timeout,
    _limit_stream_targets,
    _mark_relay_rate_limited,
    _mark_relay_stream_broken,
    _payload_has_media,
    _relay_batch_for_request,
    _relay_stream_headers,
    _should_mark_stream_broken,
    _stream_request_headers,
    _with_relay_headers,
)
from app.services.tools_dsml import (
    DSMLStreamParser,
    clean_visible_text,
    normalize_stream_tool_deltas,
    parse_dsml_tool_calls,
)
from app.services.usage import _safe_record
from app.services.upstream import (
    _classify_rate_limit,
    _relay_cooldown_seconds,
    _retry_after_seconds,
    _should_retry_same_route_429,
    call_upstream,
)


async def stream_generator(
    payload: Dict[str, Any],
    *,
    client_model: str,
    include_usage_requested: bool,
    background_tasks: BackgroundTasks,
    use_relay: bool,
    opencode_headers: Optional[Dict[str, str]] = None,
):
    """Translate upstream SSE into clean OpenAI-compatible SSE for Hermes.

    Streaming goes THROUGH the Edge-runtime relay by default so the
    upstream (OpenCode) only ever sees the relay IPs. Failover between
    relays (and optional direct fallback) happens ONLY before the first
    payload chunk reaches the client; after that, a retry would duplicate
    content, so mid-stream failures terminate with an error chunk.
    """

    parser = DSMLStreamParser()
    sent_role = False
    call_index = 0
    saw_tool_call = False
    saw_text_content = False
    sent_payload = False  # True once a real chunk reached the client
    stream_completed = False  # True when a target's stream finished naturally
    last_usage: Optional[Dict[str, Any]] = None
    reasoning_buffer: List[str] = []
    reasoning_buffer_chars = 0
    stream_id = f"chatcmpl-{secrets.token_hex(16)}"
    created = int(time.time())
    stream_start = time.time()  # for total-stream timeout
    last_finish_reason: Optional[str] = None

    # ---- phase timing diagnostics ----
    first_reasoning_at: Optional[float] = None
    last_reasoning_at: Optional[float] = None
    reasoning_chars = 0
    first_content_at: Optional[float] = None
    last_content_at: Optional[float] = None
    content_chunks = 0
    content_chars = 0
    wire_bytes = 0
    parsed_chunks = 0

    def record_final_usage() -> None:
        """Record usage ONCE per stream, using the final cumulative value.

        Upstream providers (including the opencode relay) commonly send a
        cumulative `usage` object on every SSE chunk when
        `stream_options.include_usage` is set. Recording each chunk would
        inflate token totals by the number of chunks, so we keep the last
        value and persist a single row per stream.
        """
        if not last_usage:
            return
        try:
            prompt_tokens = int(last_usage.get("prompt_tokens", 0) or 0)
            completion_tokens = int(last_usage.get("completion_tokens", 0) or 0)
            total_tokens = int(last_usage.get("total_tokens", 0) or 0)
        except (TypeError, ValueError):
            # Upstream kadang mengirim angka sebagai string tak valid/None.
            # Jangan biarkan ValueError di sini membunuh stream; skip pencatatan.
            _log("USAGE", f"Skipping usage record with invalid values: {last_usage!r}")
            return
        background_tasks.add_task(
            _safe_record,
            request_id=stream_id,
            model=client_model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
        )

    def chunk(delta: Dict[str, Any], finish_reason: Optional[str] = None) -> str:
        return _sse(
            {
                "id": stream_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": client_model,
                "choices": [
                    {"index": 0, "delta": delta, "finish_reason": finish_reason},
                ],
            }
        )

    def role_chunk() -> str:
        nonlocal sent_role
        if sent_role:
            return ""
        sent_role = True
        return chunk({"role": "assistant"})

    def mark_content() -> None:
        """Record that a visible content/tool_call chunk reached the client."""
        nonlocal first_content_at, last_content_at, content_chunks
        now = time.time()
        if first_content_at is None:
            first_content_at = now
            suffix = " (reasoning phase ended)" if first_reasoning_at is not None else ""
            _log("STREAM", f"CONTENT start +{now - stream_start:.2f}s{suffix}")
        last_content_at = now
        content_chunks += 1

    def _fmt_phase(ts: Optional[float]) -> str:
        return f"+{ts - stream_start:.2f}s" if ts is not None else "-"

    def log_phase_summary(note: str = "") -> None:
        """Log one line with the reasoning/content phase breakdown.

        Diagnostik ini dipanggil dari jalur error (all-failed/lost/error),
        sehingga TIDAK BOLEH melempar: exception di sini akan menutupi error
        asli (kasus NameError wire_bytes sebelumnya). Seluruh body dibungkus
        try/except defensif.
        """
        try:
            now = time.time()
            if first_reasoning_at is not None and last_reasoning_at is not None:
                reasoning_dur = f"{last_reasoning_at - first_reasoning_at:.2f}s"
            else:
                reasoning_dur = "-"
            if first_content_at is not None and last_content_at is not None:
                content_dur = f"{last_content_at - first_content_at:.2f}s"
            else:
                content_dur = "-"
            _log(
                "STREAM",
                f"SUMMARY stream={stream_id[:8]} {note} "
                f"reasoning:start={_fmt_phase(first_reasoning_at)} dur={reasoning_dur} chars={reasoning_chars} "
                f"content:start={_fmt_phase(first_content_at)} dur={content_dur} chars={content_chars} chunks={content_chunks} "
                f"wire_bytes={wire_bytes} parsed_chunks={parsed_chunks} "
                f"total={now - stream_start:.2f}s",
            )
        except Exception:  # noqa: BLE001 - diagnostic must never crash the stream
            pass

    # SSE must stay uncompressed end-to-end. See _stream_request_headers().
    base_headers = _stream_request_headers(opencode_headers)

    # Susun daftar kandidat target: semua relay (round-robin), lalu opsional
    # fallback direct. Path non-streaming tetap memakai call_upstream.
    targets: List[Tuple[str, Dict[str, str]]] = []
    if use_relay:
        for relay_url in _relay_batch_for_request(for_stream=True):
            targets.append((relay_url, _relay_stream_headers(base_headers)))
        if RELAY_FALLBACK:
            targets.append((OPENCODE_URL, dict(base_headers)))
    else:
        targets.append((OPENCODE_URL, dict(base_headers)))
    targets = _limit_stream_targets(targets)
    vision_direct_first = use_relay and RELAY_FALLBACK and _payload_has_media(payload)
    if vision_direct_first:
        # Vision: direct DULU; relay HANYA fallback bila direct 429
        # (lihat guard di loop). Biner base64 rawan 413/504 relay.
        direct = [t for t in targets if "x-relay-target" not in t[1]]
        relays = [t for t in targets if "x-relay-target" in t[1]]
        targets = direct + relays
        _log("STREAM", f"VISION model={client_model}: direct dulu, relay khusus 429")

    last_error: Optional[str] = None
    last_rate_limited = False  # True bila kegagalan terakhir adalah 429
    last_retry_after: Optional[float] = None  # dari header Retry-After upstream
    # Bug spam-429 opencode khusus muse-spark: tiap target boleh dicoba 1x
    # lagi ke route YANG SAMA sebelum dirotasi (dilacak di set ini).
    spurious_429_retried: set = set()
    _log("STREAM", f"REQ model={client_model} keys={sorted(payload.keys())} {_oc_session_tag(opencode_headers)}")

    try:
        # Setiap 429/error LANGSUNG dirotasi ke target berikutnya (tidak
        # retry pada IP yang sama) supaya tiap attempt memakai relay/IP yang
        # berbeda. Retry hanya dilakukan pada level request berikutnya.
        # PENGECUALIAN: muse-spark (Responses-only) retry 1x same-route dulu.
        target_index = 0
        while target_index < len(targets):
            target_url, headers = targets[target_index]
            is_relay = "x-relay-target" in headers
            if vision_direct_first and is_relay and not last_rate_limited:
                # Relay vision hanya untuk 429 direct; kegagalan lain
                # selesai di direct (last_error sudah terisi).
                break
            _log("STREAM",
                f"ATTEMPT {target_index + 1}/{len(targets)} "
                f"{'RELAY' if is_relay else 'DIRECT'} target={target_url}"
            )
            try:
                target_termination_seen = False
                relay_target_failed = False
                client = _get_http()
                async with client.stream(
                    "POST", target_url, json=payload, headers=headers
                ) as response:
                    if response.status_code != 200:
                        try:
                            body = await response.aread()
                            detail = body.decode("utf-8", errors="replace")[:500]
                        except Exception:  # noqa: BLE001
                            detail = ""
                        if is_relay and _is_relay_timeout(response):
                            # 504 Edge dari Vercel: tidak ada byte dalam 25 dtk.
                            # Untuk konteks raksasa ini WAJAR (TTFB>25s) -> JANGAN
                            # tandai broken (relay sehat); untuk request normal
                            # tandai agar streaming berikut skip relay ini.
                            if _should_mark_stream_broken(target_url, payload):
                                _mark_relay_stream_broken(
                                    target_url,
                                    time.time() + RELAY_STREAM_BROKEN_COOLDOWN,
                                )
                            last_error = f"Relay stream timeout (504): {detail[:200]}"
                            target_index += 1
                            _log(
                                "STREAM",
                                f"STREAM-TIMEOUT {target_url} | Vercel kill 504 "
                                f"(limit eksekusi ~25s, bukan bug proxy) -> relay "
                                f"di-skip streaming {RELAY_STREAM_BROKEN_COOLDOWN:.0f}s, "
                                f"lanjut ke target berikutnya/direct",
                            )
                            continue
                        if response.status_code == 429 and not sent_payload:
                            # 429 = IP relay sedang dibatasi (baik dari Vercel
                            # sendiri maupun OpenCode via relay). Menunggu/
                            # retry pada IP yang sama hampir selalu 429 lagi.
                            # Langsung ROTASI: relay dicatat masuk cooldown
                            # (agar request berikutnya mulai dari relay yang
                            # sehat) lalu pindah ke target berikutnya.
                            # PENGECUALIAN bug spam-429 muse-spark: retry 1x
                            # same-route dulu sebelum rotasi/cooldown.
                            if _should_retry_same_route_429(client_model) and target_url not in spurious_429_retried:
                                spurious_429_retried.add(target_url)
                                _delay = _retry_after_seconds(response, RATE_LIMIT_BACKOFF)
                                _log(
                                    "STREAM",
                                    f"SPURIOUS-429 {target_url} | retry same-route 1x "
                                    f"in {_delay:.1f}s sebelum ganti route",
                                )
                                await asyncio.sleep(_delay)
                                continue  # ulangi target_index yang sama
                            rate_cls = _classify_rate_limit(response)
                            last_retry_after = _retry_after_seconds(
                                response, RATE_LIMIT_BACKOFF
                            )
                            _mark_relay_rate_limited(
                                target_url,
                                time.time() + _relay_cooldown_seconds(response),
                            )
                            last_error = f"Rate limited (429): {detail}"
                            last_rate_limited = True
                            _log(
                                "STREAM",
                                f"RATE-LIMITED {target_url} | "
                                f"{rate_cls['description']} | IP relay dirotasi "
                                f"terus -> target berikutnya | detail={detail[:300]!r}",
                            )
                            target_index += 1
                            _log("STREAM",
                                f"FAIL {target_url} | upstream-status=429 detail={detail[:300]!r}"
                            )
                            continue  # target berikutnya
                        last_error = (
                            f"Upstream responded with {response.status_code}: {detail}"
                        )
                        last_rate_limited = response.status_code == 429
                        target_index += 1
                        _log("STREAM",
                            f"FAIL {target_url} "
                            f"| upstream-status={response.status_code} detail={detail[:300]!r}"
                        )
                        continue  # target berikutnya

                    _log("STREAM", f"OK {target_url}")

                    line_iter = response.aiter_lines()
                    pending_line_task: Optional[asyncio.Task[str]] = None
                    last_activity_at = time.time()
                    # wire_bytes / parsed_chunks diakumulasi antar-target
                    # (diinisialisasi 0 di awal stream_generator agar
                    # log_phase_summary aman dipanggil sebelum target OK).
                    # Peringatan: kalau terlihat SERPIHAN panjang (mis. >30s)
                    # tapi parsed_chunks tidak naik, berarti relay/middlebox
                    # menahan byte (buffer) — streaming tidak benar-benar
                    # pass-through, dan idle timeout TIDAK akan pernah memicu
                    # karena socket tetap "aktif".
                    try:
                        while True:
                            # Idle timeout: landasanya adalah PROGRESS, bukan
                            # durasi total. Selama upstream terus mengirim byte
                            # (reasoning/content/komentar), stream dibiarkan
                            # berjalan. Hanya kalau tidak ada data sama sekali
                            # selama REQUEST_TIMEOUT detik, stream dibunuh.
                            idle_seconds = time.time() - last_activity_at
                            if idle_seconds > REQUEST_TIMEOUT:
                                raise asyncio.TimeoutError(
                                    f"No upstream progress for {int(idle_seconds)}s"
                                    f" (idle timeout {REQUEST_TIMEOUT}s)"
                                )

                            # Do not wrap __anext__() in wait_for(). wait_for()
                            # cancels the iterator when the keepalive timer fires;
                            # cancelling an async generator mid-read can make the
                            # next __anext__() fail and kill a healthy stream.
                            if pending_line_task is None:
                                pending_line_task = asyncio.create_task(
                                    line_iter.__anext__()
                                )

                            done, _ = await asyncio.wait(
                                (pending_line_task,),
                                timeout=min(
                                    SSE_KEEPALIVE_INTERVAL,
                                    max(0.0, REQUEST_TIMEOUT - idle_seconds),
                                ),
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if not done:
                                yield ":\n\n"
                                continue

                            try:
                                line = pending_line_task.result()
                            except StopAsyncIteration:
                                # Upstream menutup stream SSE. Sumber yang
                                # seharusnya menutup dengan [DONE] atau chunk
                                # finish_reason. Beberapa relay/provider memotong
                                # koneksi tanpa sinyal terminasi; jika payload
                                # nyata sudah terkirim, akhiri dengan GRACEFUL
                                # (flush sisa + finish + [DONE]) alih-alih
                                # membuang seluruh jawaban dengan error chunk.
                                if not target_termination_seen:
                                    if sent_payload:
                                        _log(
                                            "STREAM",
                                            f"EOF {target_url} without "
                                            f"[DONE]/finish_reason - completing "
                                            f"gracefully",
                                        )
                                    else:
                                        raise httpx.ReadError(
                                            "Upstream stream ended before completion"
                                        )
                                stream_completed = True
                                break
                            finally:
                                pending_line_task = None

                            if not line:
                                continue
                            if not line.startswith("data:"):
                                continue

                            data = line[5:].lstrip()
                            wire_bytes += len(data)
                            if data == "[DONE]":
                                target_termination_seen = True
                                stream_completed = True
                                break

                            try:
                                upstream_chunk = json.loads(data)
                            except json.JSONDecodeError:
                                continue
                            # Relay early-SSE (core.js baru): fetch upstream
                            # gagal di background dilaporkan sebagai event,
                            # karena status HTTP sudah terlanjur 200. Tanpa
                            # cegatan ini, failure dikira sukses kosong.
                            if (
                                isinstance(upstream_chunk, dict)
                                and upstream_chunk.get("type") == "relay.error"
                                and not sent_payload
                            ):
                                relay_status = upstream_chunk.get("status")
                                relay_body = str(upstream_chunk.get("body") or "")[:300]
                                if relay_status == 429:
                                    last_retry_after = RATE_LIMIT_BACKOFF
                                    last_rate_limited = True
                                    if is_relay:
                                        _mark_relay_rate_limited(
                                            target_url,
                                            time.time() + RATE_LIMIT_COOLDOWN,
                                        )
                                else:
                                    last_rate_limited = relay_status == 429
                                last_error = (
                                    f"Relay error {relay_status}: {relay_body}"
                                )
                                relay_target_failed = True
                                _log(
                                    "STREAM",
                                    f"RELAY-ERROR {target_url} | status="
                                    f"{relay_status} detail={relay_body!r} "
                                    f"-> target berikutnya",
                                )
                                break
                            parsed_chunks += 1
                            # Progress dihitung dari CHUNK DATA nyata, bukan dari
                            # komentar/keepalive SSE. Kalau upstream hanya terus
                            # mengirim keepalive tanpa konten, idle timeout akan
                            # memicu dan klien tahu stream macet, bukan hang.
                            last_activity_at = time.time()

                            if isinstance(upstream_chunk, dict):
                                upstream_usage = upstream_chunk.get("usage")
                                if isinstance(upstream_usage, dict) and upstream_usage:
                                    # Upstream sering mengirim usage KUMULATIF di
                                    # hampir semua chunk. Simpan nilai terakhir
                                    # dan rekam SEKALI di akhir stream, bukan per
                                    # chunk (mencegah inflasi token & request).
                                    # Tanpa `continue`: chunk yang sekaligus
                                    # membawa choices/finish_reason tetap diproses
                                    # di bawah, sehingga delta terakhir dan sinyal
                                    # terminasi tidak tertelan.
                                    last_usage = upstream_usage

                            choices = upstream_chunk.get("choices")
                            if not isinstance(choices, list) or not choices:
                                continue

                            choice = choices[0] if isinstance(choices[0], dict) else {}
                            if choice.get("finish_reason") is not None:
                                last_finish_reason = choice.get("finish_reason")
                                target_termination_seen = True
                            delta = choice.get("delta") or {}
                            if not isinstance(delta, dict):
                                continue

                            native_call_deltas = normalize_stream_tool_deltas(
                                delta.get("tool_calls")
                            )
                            visible_text = delta.get("content")
                            if visible_text is not None and not isinstance(
                                visible_text, str
                            ):
                                visible_text = str(visible_text)

                            # Reasoning-capable free models may send
                            # token di `reasoning_content`; `content` bisa null
                            # sampai akhir (atau seluruh max_tokens habis untuk
                            # reasoning). Tampung sebagai fallback anti respons
                            # kosong: hanya dipakai bila stream berakhir tanpa
                            # konten maupun tool call.
                            reasoning_text = delta.get("reasoning_content")
                            if reasoning_text is None:
                                # Some OpenAI-compatible providers stream
                                # thinking under the newer `reasoning` field
                                # name instead of `reasoning_content`.
                                reasoning_text = delta.get("reasoning")
                            if reasoning_text is not None and not isinstance(
                                reasoning_text, str
                            ):
                                reasoning_text = str(reasoning_text)
                            if reasoning_text:
                                # Buffer HANYA fallback anti-empty-response: batasi
                                # ~20rb char terakhir agar stream reasoning panjang
                                # tidak menumpuk memori per request.
                                reasoning_buffer.append(reasoning_text)
                                reasoning_buffer_chars += len(reasoning_text)
                                while len(reasoning_buffer) > 1 and reasoning_buffer_chars > 20000:
                                    reasoning_buffer_chars -= len(reasoning_buffer.pop(0))
                                now = time.time()
                                reasoning_chars += len(reasoning_text)
                                if first_reasoning_at is None:
                                    first_reasoning_at = now
                                    _log(
                                        "STREAM",
                                        f"REASONING start +{now - stream_start:.2f}s",
                                    )
                                last_reasoning_at = now
                                # Forward reasoning deltas so the socket stays
                                # active during long thinking phases. Clients
                                # that do not support `reasoning_content` simply
                                # ignore the field.
                                if REASONING_FORWARD:
                                    role = role_chunk()
                                    if role:
                                        sent_payload = True
                                        yield role
                                    sent_payload = True
                                    yield chunk(
                                        {"reasoning_content": reasoning_text}
                                    )

                            filtered_text = ""
                            dsml_calls: List[Dict[str, Any]] = []
                            if visible_text:
                                filtered_text, dsml_calls = parser.feed(visible_text)

                            calls = native_call_deltas or dsml_calls
                            if filtered_text or calls:
                                role = role_chunk()
                                if role:
                                    sent_payload = True
                                    yield role

                            if filtered_text:
                                sent_payload = True
                                saw_text_content = True
                                content_chars += len(filtered_text)
                                mark_content()
                                yield chunk({"content": filtered_text})

                            if native_call_deltas:
                                saw_tool_call = True
                                sent_payload = True
                                mark_content()
                                yield chunk({"tool_calls": native_call_deltas})
                            else:
                                for tool_call in dsml_calls:
                                    saw_tool_call = True
                                    sent_payload = True
                                    mark_content()
                                    yield chunk(
                                        {
                                            "tool_calls": [
                                                {
                                                    "index": call_index,
                                                    "id": tool_call["id"],
                                                    "type": "function",
                                                    "function": tool_call["function"],
                                                }
                                            ]
                                        }
                                    )
                                    call_index += 1
                    finally:
                        # If the request is cancelled or the total stream
                        # timeout fires while waiting for a line, do not leave
                        # a reader task behind after the response is closed.
                        if pending_line_task is not None:
                            pending_line_task.cancel()
                            with suppress(asyncio.CancelledError, Exception):
                                await pending_line_task

                if relay_target_failed:
                    # relay.error sebelum payload: bukan sukses — rotasi ke
                    # target berikutnya (failover tetap jalan walau HTTP 200).
                    target_index += 1
                    continue

                # SUCCESS. Inner `async with client.stream(...)` sudah
                # tertutup. Set stream_completed dan HENTIKAN loop target,
                # sehingga kita tidak connect ke target berikutnya (yang
                # akan menyebabkan klien menerima chunk duplikat ATAU
                # error chunk "Stream connection lost" padahal stream
                # pertama sudah selesai normal).
                stream_completed = True
                break  # keluar dari loop target

            except (
                httpx.TimeoutException,
                httpx.ConnectError,
                httpx.ReadError,
                httpx.RemoteProtocolError,
            ) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                _log("STREAM", f"FAIL {target_url} ({type(exc).__name__})")
                if sent_payload:
                    # Sudah ada konten sampai ke klien; retry akan
                    # menduplikasi teks. Akhiri dengan error yang jelas.
                    record_final_usage()
                    log_phase_summary("lost")
                    try:
                        yield _sse(
                            {
                                "error": {
                                    "message": "Stream connection lost",
                                    "detail": str(exc),
                                }
                            }
                        )
                        yield "data: [DONE]\n\n"
                    except (GeneratorExit, asyncio.CancelledError):
                        raise
                    except Exception:
                        # Klien sudah putus di tengah yield error-chunk;
                        # jangan lempar error baru dari dalam handler.
                        pass
                    return
                target_index += 1
                continue  # belum ada payload terkirim -> aman coba target lain

        if not stream_completed:
            log_phase_summary("all-failed")
            if last_rate_limited:
                # Beri pesan 429 yang jelas + perkiraan Retry-After agar klien
                # (Hermes/SDK) menunggu alih-alih langsung mencoba ulang dan
                # memicu spam exception.
                yield _sse(
                    {
                        "error": {
                            "message": "Upstream rate limited (429)",
                            "detail": last_error or "Rate limited",
                            "code": "RATE_LIMITED",
                            "retry_after": last_retry_after or RATE_LIMIT_BACKOFF,
                        }
                    }
                )
            else:
                yield _sse(
                    {
                        "error": {
                            "message": "All stream targets failed",
                            "detail": last_error or "unknown error",
                        }
                    }
                )
            yield "data: [DONE]\n\n"
            return

        remaining = parser.flush()
        if remaining:
            role = role_chunk()
            if role:
                yield role
            saw_text_content = True
            content_chars += len(remaining)
            mark_content()
            yield chunk({"content": remaining})

        # Fallback anti-empty-response: bila stream berakhir tanpa konten
        # maupun tool call (mis. seluruh max_tokens habis untuk reasoning),
        # teruskan reasoning sebagai isi agar klien tidak menerima respons
        # kosong. DSML di dalam reasoning ikut dibersihkan.
        if not saw_text_content and not saw_tool_call and reasoning_buffer:
            reasoning_visible, _ = parse_dsml_tool_calls("".join(reasoning_buffer))
            reasoning_visible = clean_visible_text(reasoning_visible)
            if reasoning_visible:
                role = role_chunk()
                if role:
                    sent_payload = True
                    yield role
                sent_payload = True
                content_chars += len(reasoning_visible)
                mark_content()
                yield chunk({"content": reasoning_visible})

        if not sent_role:
            yield role_chunk()

        # Rekam usage final — SATU baris per stream.
        record_final_usage()

        # Teruskan finish_reason asli upstream (mis. "length" saat max_tokens
        # habis) agar klien tahu jawaban terpotong, bukan menganggapnya selesai.
        finish_reason = "tool_calls" if saw_tool_call else (last_finish_reason or "stop")
        yield chunk({}, finish_reason)
        if last_usage and include_usage_requested:
            sent_payload = True
            yield _sse(
                {
                    "id": stream_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": client_model,
                    "choices": [],
                    "usage": last_usage,
                }
            )
        # Log breakdown sekali sebelum [DONE]. Sertakan finish_reason upstream
        # agar bisa dibedakan selesai-normal vs terpotong (length).
        log_phase_summary(f"done finish={last_finish_reason or finish_reason}")
        yield "data: [DONE]\n\n"

    except asyncio.CancelledError:
        # Client disconnect/request cancellation is an expected lifecycle event.
        # Re-raise it without logging; uvicorn/Starlette already handles it and
        # logging it here creates exception spam during normal client retries.
        raise
    except GeneratorExit:
        # Generator closure is also expected when a streaming client disconnects.
        raise
    except Exception as exc:  # noqa: BLE001 - last resort safety net
        try:
            _log("ERROR", f"Stream error: {type(exc).__name__}")
            log_phase_summary("error")
            # Keep the stream error bounded; do not expose exception text repeatedly.
            yield _sse({"error": {"message": "Stream error", "code": "STREAM_ERROR"}})
            yield "data: [DONE]\n\n"
        except (GeneratorExit, asyncio.CancelledError):
            raise
        except Exception:
            pass

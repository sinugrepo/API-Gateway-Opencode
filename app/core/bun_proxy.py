"""Bun sidecar subprocess manager (egress fingerprint TLS asli).

Mengelola lifecycle proses `bun bun-sidecar.js`:
- launch saat app start (lifespan), health-check port siap, auto-restart
  bila crash (supervisor loop sederhana).
- expose `bun_sidecar_live()` (True bila proses + port responsive) dan
  `bun_forward(...)` (helper forward satu request ke sidecar via TCP
  localhost).
- Bila Bun tidak tersedia / mati -> semua helper mengembalikan None/False
  sehingga pemanggil request-path TIDAK crash, dan otomatis fallback ke
  httpx/socksio (perilaku lama).

Desain anti-bug: TIDAK ada exception yang naik ke request path. Semua
kondisi kegagalan (bun tidak ada, port tidak siap, koneksi putus, sidecar
jawab ERROR) diterjemahkan menjadi `bun_forward()` -> None agar pemanggil
langsung pakai jalur httpx.
"""
import asyncio
import os
import shlex
import socket
import subprocess
import threading
import time
from typing import Dict, Optional

from app.core.config import (
    BUN_SIDECAR_CMD,
    BUN_SIDECAR_DIRECT_ONLY,
    BUN_SIDECAR_ENABLED,
    BUN_SIDECAR_HOST,
    BUN_SIDECAR_PORT,
    BUN_SIDECAR_SCRIPT,
    BUN_SIDECAR_START_TIMEOUT,
)
from app.core.logging_utils import _log


_proc: Optional[subprocess.Popen] = None
_proc_lock = threading.Lock()
_supervisor_stop = threading.Event()
_supervisor_thread: Optional[threading.Thread] = None
# True bila sidecar terverifikasi hidup + port responsive (perubahan hanya
# di satu titik: health-check, dibaca oleh path request secara thread-safe
# karena bool immutable).
_sidecar_live = False
_live_lock = threading.Lock()


def _enabled() -> bool:
    return BUN_SIDECAR_ENABLED


def _command() -> list:
    """Bangun argv launch. BUN_SIDECAR_CMD menang bila diisi."""
    if BUN_SIDECAR_CMD:
        return shlex.split(BUN_SIDECAR_CMD)
    return ["bun", BUN_SIDECAR_SCRIPT]


def _port_ready(host: str, port: int, timeout_s: float = 0.2) -> bool:
    """Soket ke port sidecar; True bila bisa connect (menyatakan siap)."""
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except (OSError, ValueError):
        return False


def _mark_live(live: bool) -> None:
    global _sidecar_live
    with _live_lock:
        _sidecar_live = bool(live)


def _proc_alive() -> bool:
    with _proc_lock:
        p = _proc
        return p is not None and p.poll() is None


def _launch() -> bool:
    """Launch satu kali + tunggu port siap. True bila siap."""
    global _proc
    cmd = _command()
    env = dict(os.environ)
    env["BUN_PROXY_PORT"] = str(BUN_SIDECAR_PORT)
    env["BUN_PROXY_HOST"] = BUN_SIDECAR_HOST
    try:
        with _proc_lock:
            if _proc_alive():
                return _port_ready(BUN_SIDECAR_HOST, BUN_SIDECAR_PORT)
            _proc = subprocess.Popen(
                cmd,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
        # Tunggu port siap (dengan batas waktu).
        deadline = time.time() + max(0.5, BUN_SIDECAR_START_TIMEOUT)
        while time.time() < deadline:
            if _port_ready(BUN_SIDECAR_HOST, BUN_SIDECAR_PORT):
                _mark_live(True)
                _log("BUN", f"SIDECAR siap pada {BUN_SIDECAR_HOST}:{BUN_SIDECAR_PORT} (cmd={cmd})")
                return True
            time.sleep(0.1)
        _log("BUN", f"SIDECAR belum siap setelah {BUN_SIDECAR_START_TIMEOUT:.0f}s; fallback httpx")
        _mark_live(False)
        return False
    except (OSError, FileNotFoundError) as exc:
        _log("BUN", f"SIDECAR launch gagal ({type(exc).__name__}: {exc}); fallback httpx")
        _mark_live(False)
        return False


def _supervisor_loop() -> None:
    """Restart sidecar bila mati (hanya selama app masih jalan)."""
    while not _supervisor_stop.is_set():
        if not _proc_alive():
            _mark_live(False)
            if _launch():
                continue
            # Belum siap: beri jeda agar tidak spin crash-loop.
            _supervisor_stop.wait(2.0)
            continue
        # Proses hidup: cek port sekali-sekala, tandai mati bila port putus.
        time.sleep(3.0)
        if _proc_alive() and _port_ready(BUN_SIDECAR_HOST, BUN_SIDECAR_PORT):
            _mark_live(True)
        else:
            _log("BUN", "SIDECAR mati/port putus -> akan di-restart supervisor")
            _mark_live(False)
            _supervisor_stop.wait(1.0)


def start() -> None:
    """Panggil di lifespan startup. Tidak melempar."""
    if not _enabled():
        return
    _supervisor_stop.clear()
    global _supervisor_thread
    with _proc_lock:
        if _supervisor_thread is not None and _supervisor_thread.is_alive():
            return
        _supervisor_thread = threading.Thread(
            target=_supervisor_loop, name="bun-sidecar-supervisor", daemon=True
        )
        _supervisor_thread.start()
    _log("BUN", f"SIDECAR supervisor start (enabled=True, direct_only={BUN_SIDECAR_DIRECT_ONLY})")


def stop() -> None:
    """Panggil di lifespan shutdown: hentikan supervisor + bunuh proses."""
    global _proc
    _supervisor_stop.set()
    with _proc_lock:
        p = _proc
        _proc = None
    if p is not None and p.poll() is None:
        try:
            p.terminate()
            p.wait(timeout=3)
        except (OSError, ValueError, subprocess.SubprocessError):
            try:
                p.kill()
            except (OSError, ValueError, subprocess.SubprocessError):
                pass
    _log("BUN", "SIDECAR shutdown")


def live() -> bool:
    """True bila enabled DAN proses hidup DAN port responsive. Thread-safe."""
    if not _enabled():
        return False
    if not _proc_alive():
        return False
    # Port check ringan; jangan terlalu sering (dipakai per-attempt).
    return _port_ready(BUN_SIDECAR_HOST, BUN_SIDECAR_PORT, timeout_s=0.15)


def handle_error(reason: str) -> None:
    """Catat + tandai mati sidecar bila sidecar membalas ERROR transport.

    Dipanggil oleh pemakai `forward()` agar jalur berikutnya langsung
    fallback httpx (tanpa harus menunggu supervisor).
    """
    if reason.startswith("proxy-not-supported-in-bun"):
        # Runtime Bun tidak punya agent proxy yang dibutuhkan. Ini kondisi
        # persisten: tandai mati agar tidak di-coba lagi (hingga restart).
        _log("BUN", f"SIDECAR jawab {reason} -> nonaktifkan jalur Bun (proxy TLS tidak didukung runtime ini)")
        _mark_live(False)
        return
    _log("BUN", f"SIDECAR forward gagal: {reason} -> fallback httpx untuk attempt ini")


def bun_response(result: Optional[Dict[str, object]]) -> Optional["httpx.Response"]:
    """Ubah hasil `forward()` (dict) menjadi httpx.Response palsu agar pemanggil
    request-path (yang memakai `.status_code`, `.headers`, `.text`) tetap jalan
    tanpa perubahan. Return None bila result None (fallback httpx)."""
    if result is None:
        return None
    import httpx

    status = int(result.get("status", 500))
    body = result.get("body") or b""
    if isinstance(body, str):
        body = body.encode("utf8", "replace")
    headers = result.get("headers") or {}
    resp = httpx.Response(
        status_code=status,
        content=body,
        headers=headers,
        request=httpx.Request("POST", str(result.get("url", ""))),
    )
    # Retry-After dari status line (jika tidak ada di headers).
    retry_after = result.get("retry_after")
    if retry_after and float(retry_after or 0) > 0:
        resp.headers["retry-after"] = str(retry_after)
    return resp


# ---------------------------------------------------------------------------
# Forward satu request ke sidecar via TCP localhost.
# ---------------------------------------------------------------------------
def forward(
    method: str,
    target_url: str,
    headers: Dict[str, str],
    body: Optional[bytes] = None,
    proxy_spec: str = "direct",
    timeout_s: float = 120.0,
) -> Optional[Dict[str, object]]:
    """Forward satu request non-stream; return
    {"status","retry_after","headers","body"} atau None (fallback httpx).

    Dipakai hanya untuk request NON-STREAM (body response di-buffer utuh).
    Stream SSE memakai jalur relay/direct httpx seperti biasa — sidecar
    dirancang untuk fingerprint egress, bukan untuk mengganti seluruh
    mesin streaming gateway.
    """
    if not live():
        return None
    import socket as _socket

    sock = None
    try:
        sock = _socket.create_connection(
            (BUN_SIDECAR_HOST, BUN_SIDECAR_PORT), timeout=max(1.0, timeout_s)
        )
        sock.settimeout(max(1.0, timeout_s))
        body = body or b""
        hdr_lines = "\n".join(
            f"{k}: {v}" for k, v in (headers or {}).items() if v is not None
        )
        first = f"{method} {target_url} {len(body)} {proxy_spec}\n"
        sock.sendall((first + hdr_lines + "\n").encode("utf8"))
        if body:
            sock.sendall(body)

        # Baca bertahap: status line -> header block -> body.
        buf = b""
        # 1) STATUS line
        while not buf.endswith(b"\n"):
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf += chunk
        status_line, _, rest = buf.partition(b"\n")
        parts = status_line.decode("utf8", "replace").split(" ")
        if parts and parts[0] == "ERROR":
            handle_error(parts[1] if len(parts) > 1 else "unknown")
            return None
        if len(parts) < 2:
            handle_error("bad-status-line")
            return None
        status = int(parts[1])
        retry_after = (
            float(parts[2])
            if len(parts) > 2 and parts[2].replace(".", "", 1).isdigit()
            else 0.0
        )

        # 2) Header block: sampai empty line (\n\n) — boleh butuh recv lagi.
        while b"\n\n" not in rest:
            chunk = sock.recv(65536)
            if not chunk:
                break
            rest += chunk
        if b"\n\n" in rest:
            header_block, body_start = rest.split(b"\n\n", 1)
        else:
            header_block, body_start = rest, b""
        resp_headers: Dict[str, str] = {}
        for line in header_block.decode("utf8", "replace").split("\n"):
            if not line or ":" not in line:
                continue
            k, _, v = line.partition(":")
            resp_headers[k.strip().lower()] = v.strip()

        resp_headers: Dict[str, str] = {}
        for line in header_block.decode("utf8", "replace").split("\n"):
            if not line or ":" not in line:
                continue
            k, _, v = line.partition(":")
            resp_headers[k.strip().lower()] = v.strip()

        # 3) Body: `body_start` + semua sisa yang masih di-buffer.
        body_buf = body_start
        return {
            "status": status,
            "retry_after": retry_after,
            "headers": resp_headers,
            "body": body_buf,
            "url": target_url,
        }
    except (OSError, ValueError) as exc:
        handle_error(f"{type(exc).__name__}: {exc}")
        return None
    finally:
        if sock is not None:
            try:
                sock.close()
            except (OSError, ValueError):
                pass

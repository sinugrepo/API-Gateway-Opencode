"""OpenCode CLI identity headers (free-tier session).

Format ID mengikuti reverse-engineer `kode-ai/providers/opencode/headers.go`
(lihat `opencode-session.md` §4):
  suffix 26 char = 12 hex (timestamp ms*0x1000+counter, big-endian [2:],
  descending=bitwise-NOT untuk session) + 14 base62 acak (`0-9A-Za-z`).
  Session: `ses_` + suffix(descending=True)
  Request: `msg_` + suffix(descending=False, unik per POST)

Aturan pakai (§7): session STABIL 1 ID per conversation (cache affinity),
request UNIK per message, project=global, client=cli.
"""
import hashlib
import json
import os
import re
import secrets
import struct
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

from app.core.config import OPENCODE_CLIENT_NAME, OPENCODE_PROJECT, OPENCODE_SESSION_ID
from app.services.model_context import get_model_context_window


_BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

# User-Agent ala CLI opencode v2 (bisa dioverride via env).
# Gate upstream v2 (live-bisect 2026-10-14, endpoint /inference/v1/*):
# UA WAJIB bentuk `opencode/<channel>/<version>/<client>` — UA gaya lama
# `opencode/1.19.0` (tanpa channel/client) berisiko ditolak. Default
# `opencode/latest/2.0.26/cli` = nilai terbukti lolos gate.
OPENCODE_USER_AGENT = os.getenv(
    "OPENCODE_USER_AGENT",
    "opencode/latest/2.0.26/cli",
)


def _random_base62(n: int) -> str:
    """N char base62 acak (`0-9A-Za-z`), urutan alfabet sesuai Go spec."""
    token = secrets.token_bytes(n)
    return "".join(_BASE62_ALPHABET[b % 62] for b in token)


def _create_id(descending: bool, t: Optional[float] = None, counter: int = 1) -> str:
    """Suffix 26 char: 12 hex timestamp + 14 base62 acak (ekuivalen Go createID)."""
    ms = int((t if t is not None else time.time()) * 1000)
    now = ((ms * 0x1000 + counter) & ((1 << 64) - 1))
    if descending:
        now = (~now) & ((1 << 64) - 1)
    timestamp_hex = struct.pack(">Q", now)[2:].hex()
    return timestamp_hex + _random_base62(14)


def _new_opencode_session_id() -> str:
    return "ses_" + _create_id(True)


def _new_opencode_request_id() -> str:
    return "msg_" + _create_id(False)


def _opencode_cli_headers(
    *,
    session_id: Optional[str] = None,
) -> Dict[str, str]:
    """Bangun header identitas CLI v2 + User-Agent ala CLI asli.

    Gate upstream /inference/v1/* (live-bisect 2026-10-14):
    - `x-opencode-session` DAN `x-opencode-session-id` WAJIB dua-duanya
      (salah satu saja -> 403 FreeTierError). Nilainya sama.
    - `x-opencode-request` TIDAK BOLEH dikirim sama sekali (ada value ->
      403 FreeTierError) — dulu CLI v1 mengunggulnya, v2 menolaknya.
    - `x-session-affinity` / `x-session-id` mirror tambahan (tidak dicek,
    dikirim untuk kompatibilitas kedua varian header CLI).
    - `x-opencode-client` / `x-opencode-project` tidak dicek, dikirim apa
      adanya untuk menyerupai CLI.
    """
    session = session_id or OPENCODE_SESSION_ID or _new_opencode_session_id()
    return {
        "x-opencode-client": OPENCODE_CLIENT_NAME,
        "x-opencode-project": OPENCODE_PROJECT,
        "x-opencode-session": session,
        "x-opencode-session-id": session,
        "x-session-affinity": session,
        "x-session-id": session,
        "User-Agent": OPENCODE_USER_AGENT,
    }


def _is_valid_opencode_suffix(suffix: str) -> bool:
    """True bila suffix 26 char = 12 hex + 14 base62 (spec §4)."""
    if not isinstance(suffix, str) or len(suffix) != 26:
        return False
    hex_part, b62_part = suffix[:12], suffix[12:]
    if any(c not in "0123456789abcdefABCDEF" for c in hex_part):
        return False
    return all(c in _BASE62_ALPHABET for c in b62_part)


def _is_valid_opencode_id(value: str, prefix: str) -> bool:
    return (
        isinstance(value, str)
        and value.startswith(prefix)
        and _is_valid_opencode_suffix(value[len(prefix):])
    )


_STABLE_SESSION_FALLBACK = "ses_" + "0" * 12 + "0" * 14  # legacy (format-valid tapi timestamp 1970 = fake); dipertahankan untuk kompat, tidak lagi dipakai sebagai default.

# Cache fingerprint -> session ID timestamp-valid (lihat _stable_opencode_session).
# Alasan: 12 hex pertama suffix ses_/msg_ BUKAN hex bebas — ia meng-encode
# timestamp (ms*0x1000+counter, big-endian [2:], descending=NOT untuk session;
# lihat `opencode-session.md` §4 / headers.go createID). Upstream dapat
# men-decode-nya dan menolak ID yang timestamp-nya acak/kuno/masa-depan
# sebagai "bukan dari CLI" (403 FreeTierError di SEMUA egress walau payload
# fingerprint lain lengkap). Implementasi lama memetakan sha256 fingerprint
# langsung menjadi prefix hex -> timestamp uniform-acak dalam jendela ~2.18
# tahun (≈89% di luar 90 hari / ≈50% masa-depan) -> terdeteksi fake.
# Fix: session stabil = ID timestamp-valid yang digenerate SEKALI per
# fingerprint lalu dipakai ulang (stabil antar-turn + timestamp selalu
# plausibel saat pertama dibuat). TTL 7 hari agar proses long-running tidak
# memakai session basi selamanya; evict LRU bila penuh.
_STABLE_SESSION_CACHE_MAX = 2000
_STABLE_SESSION_TTL_S = 7 * 24 * 3600.0
_stable_session_cache: "OrderedDict[str, Tuple[str, float]]" = OrderedDict()
_stable_session_lock = threading.Lock()


def _decode_opencode_ms(suffix12: str, descending: bool) -> Optional[int]:
    """Decode estimasi millisecond dari 12 hex pertama suffix ID ala CLI.

    Return ms (int) atau None bila bukan hex / di luar rentang tanggal wajar.
    Asumsi 2 byte teratas (bits 48-63) sama dengan waktu sekarang — valid
    untuk ID yang dibuat dalam ±1 tahun terakhir (top berubah tiap ~2.18 th).
    Tak pernah melempar.
    """
    try:
        if not isinstance(suffix12, str) or len(suffix12) < 12:
            return None
        enc48 = int(suffix12[:12], 16)
        low48 = ((~enc48) if descending else enc48) & ((1 << 48) - 1)
        now_full = (int(time.time() * 1000) * 0x1000 + 1) & ((1 << 64) - 1)
        top = (now_full >> 48) & 0xFFFF
        candidate = ((top << 48) | low48)
        # Koreksi wrap ±1 top-step bila kandidat >12 jam di masa depan
        # (ID dibuat tepat sebelum top bergulir).
        now_ms = int(time.time() * 1000)
        ms = (candidate - 1) // 0x1000
        step_ms = (1 << 48) // 0x1000
        if ms - now_ms > 12 * 3600 * 1000:
            ms -= step_ms
        elif now_ms - ms > step_ms // 2:
            ms += step_ms
        return ms
    except (TypeError, ValueError, AttributeError):
        return None


def _is_plausible_opencode_id(value: str, prefix: str, max_age_days: float = 90.0) -> bool:
    """True bila ID format-valid DAN timestamp-nya plausibel (tak kuno/masa-depan).

    Session (`ses_`, descending=True) maupun request (`msg_`, descending=False)
    didukung. Jendela: [now-max_age_days, now+1 jam]. Tak pernah melempar.
    """
    try:
        if not _is_valid_opencode_id(value, prefix):
            return False
        descending = prefix.startswith("ses_")
        ms = _decode_opencode_ms(value[len(prefix):len(prefix) + 12], descending)
        if ms is None:
            return False
        now_ms = int(time.time() * 1000)
        if ms - now_ms > 3600 * 1000:
            return False
        if now_ms - ms > max_age_days * 24 * 3600 * 1000:
            return False
        return True
    except (TypeError, ValueError, AttributeError):
        return False


def _clear_stable_session_cache() -> None:
    """Kosongkan cache sesi stabil (dipakai test/setup). Tak pernah melempar."""
    try:
        with _stable_session_lock:
            _stable_session_cache.clear()
    except (TypeError, ValueError, AttributeError):
        pass


def _conversation_fingerprint(payload: Any) -> str:
    """Fingerprint deterministik percakapan dari isi payload.

    Sumber sinyal (diutamakan berurutan, semua stabil antar-turn SATU
    percakapan yang sama):
    1. prompt_cache_key / conversation id eksplisit dari klien.
    2. system/instructions pertama (awal percakapan, tidak berubah antar-turn).
    3. Pesan user pertama (isinya tidak berubah antar-turn).

    Return hex sha256 (64 char). String kosong bila payload kosong/bukan dict.
    """
    # (Catatan: hex panjang ini HANYA fingerprint internal; _stable_opencode_session
    # yang memotong/memetakan ke format sesi ala CLI asli: ses_ + 26 alfanumerik.)
    if not isinstance(payload, dict):
        return ""
    explicit = payload.get("prompt_cache_key") or payload.get("conversation") or payload.get("conversation_id")
    if isinstance(explicit, str) and explicit.strip():
        return hashlib.sha256(explicit.strip().encode("utf-8", "replace")).hexdigest()

    # Kumpulkan teks sinyal dari bentuk Responses (input/instructions) dan
    # chat (messages). Hanya teks yang benar-benar stabil antar-turn.
    texts: List[str] = []
    for key in ("instructions", "system"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            texts.append(value)
            break
    items: Any = payload.get("input")
    if items is None:
        items = payload.get("messages")
    if isinstance(items, str):
        if items.strip():
            texts.append(items)
    elif isinstance(items, list):
        for item in items:
            if isinstance(item, str):
                texts.append(item)
                break
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).lower()
            if role not in ("user", "system"):
                continue
            content = item.get("content")
            if isinstance(content, str) and content.strip():
                texts.append(content)
            elif isinstance(content, list):
                for part in content:
                    if isinstance(part, dict) and part.get("type") in ("input_text", "text"):
                        text = part.get("text")
                        if isinstance(text, str) and text.strip():
                            texts.append(text)
                            break
            if len(texts) >= 2:
                break
    if not texts:
        return ""
    joined = "\n".join(texts)
    return hashlib.sha256(joined.encode("utf-8", "replace")).hexdigest()


def _stable_opencode_session(payload: Any = None) -> str:
    """Session ID STABIL antar-turn untuk SATU percakapan yang sama.

    Masalah yang diperbaiki: identity caller acak per-request membuat
    upstream menolak replay `reasoning.encrypted_content` pada turn kedua
    ("encrypted_content was not issued to this caller") karena konten
    terenkripsi di-issuance ke session ID yang sudah hilang.

    Format HASIL mengikuti spec `opencode-session.md` §4 agar lolos
    validasi free-tier (bukan sekadar alfanumerik acak):
      `ses_` + 12 hex timestamp + 14 base62.
    KRITIS: 12 hex pertama HARUS meng-encode timestamp valid (bukan hash
    bebas) — upstream dapat men-decode-nya; hash acak terdeteksi sebagai
    fake dan ditolak 403 di semua egress. Karena itu fungsi ini TIDAK
    memetakan hash menjadi prefix, melainkan meng-generate SATU ID
    timestamp-valid per fingerprint percakapan lalu meng-cache-nya
    (stabil antar-turn dalam satu proses + timestamp plausibel).
    TTL 7 hari + LRU 2000 entri; antar-restart sesi dibuat ulang (valid
    baru) dan auto-heal encrypted_content menanggung replay basi.

    Prioritas:
    1. OPENCODE_SESSION_ID env (statis global, opsional).
    2. Fingerprint percakapan (prompt_cache_key / system+first user msg) ->
       sesi sama otomatis untuk percakapan sama, beda untuk percakapan beda.
    3. Fallback ID valid per-proses (di-cache di bawah kunci fallback)
       bila payload tak punya sinyal sama sekali (konstan dalam satu
       proses agar test/klien stabil, valid saat pertama dibuat).
    """
    if OPENCODE_SESSION_ID:
        return OPENCODE_SESSION_ID
    fingerprint = _conversation_fingerprint(payload)
    cache_key = fingerprint if fingerprint else "__fallback__"
    try:
        now = time.time()
        with _stable_session_lock:
            hit = _stable_session_cache.get(cache_key)
            if isinstance(hit, (list, tuple)) and len(hit) == 2:
                sess, created = hit
                if (
                    isinstance(sess, str)
                    and sess.startswith("ses_")
                    and isinstance(created, (int, float))
                    and (now - float(created)) < _STABLE_SESSION_TTL_S
                ):
                    _stable_session_cache.move_to_end(cache_key)
                    return sess
            fresh = _new_opencode_session_id()
            _stable_session_cache[cache_key] = (fresh, now)
            _stable_session_cache.move_to_end(cache_key)
            while len(_stable_session_cache) > _STABLE_SESSION_CACHE_MAX:
                _stable_session_cache.popitem(last=False)
            return fresh
    except (TypeError, ValueError, AttributeError):
        pass
    try:
        return _new_opencode_session_id()
    except (TypeError, ValueError, AttributeError):
        return _STABLE_SESSION_FALLBACK


def _resolve_opencode_headers(incoming: Any = None) -> Dict[str, str]:
    """Tentukan header identitas untuk satu request klien.

    Header yang sudah dikirim klien (Hermes baru dsb.) dipakai apa adanya
    BILA formatnya valid; yang hilang diisi ala CLI. Hasilnya dipakai untuk
    SEMUA upstream attempt dalam request ini agar sesi stabil lintas retry
    relay/direct.

    Catatan v2 (2026-10-14):
    - `x-opencode-request` dari klien TIDAK diteruskan — upstream /inference
      menolak request apapun yang membawanya (403 FreeTierError).
    - Sesi klien hanya dipakai bila lolos `_is_valid_opencode_id` (12 hex
      timestamp + 14 base62); nilai di luar format itu bertrigger 403 di
      /inference sehingga diganti sesi timestamp-valid buatan proxy.
    """
    def _get(name: str) -> str:
        value = None
        if incoming is not None:
            try:
                value = incoming.get(name)
            except (AttributeError, TypeError):
                value = None
            if value is None:
                # Dict biasa case-sensitive; cari case-insensitive manual.
                try:
                    items = incoming.items()
                except (AttributeError, TypeError):
                    items = ()
                lowered = name.lower()
                for key, val in items:
                    try:
                        if str(key).lower() == lowered:
                            value = val
                            break
                    except (TypeError, ValueError):
                        continue
        return (value or "").strip() if isinstance(value, str) else ""

    session = _get("x-opencode-session")
    if session and not _is_valid_opencode_id(session, "ses_"):
        session = ""
    headers = _opencode_cli_headers(session_id=session or None)
    client = _get("x-opencode-client")
    if client:
        headers["x-opencode-client"] = client
    project = _get("x-opencode-project")
    if project:
        headers["x-opencode-project"] = project
    # x-opencode-request sengaja TIDAK diteruskan (gate v2: header itu = 403).
    return headers


def _oc_session_tag(headers: Optional[Dict[str, str]]) -> str:
    """Penanda sesi singkat untuk log (10 char pertama, aman dibagikan)."""
    try:
        session = (headers or {}).get("x-opencode-session", "")
    except (AttributeError, TypeError):
        session = ""
    return f"ses={session[:14]}" if session else "ses=-"


def _fresh_request_headers(base: Optional[Dict[str, str]]) -> Dict[str, str]:
    """Salinan headers yang DIJAMIN tanpa `x-opencode-request`.

    Gate v2 (2026-10-14): upstream /inference menolak request bernilai
    `x-opencode-request` apapun dengan 403 FreeTierError, jadi header itu
    dibuang (termasuk bila base membawanya dari klien). Sesi
    (`x-opencode-session` + mirror) sengaja TIDAK diubah agar affinity cache
    + issuance encrypted_content tetap stabil dalam satu request klien.
    """
    headers = dict(base or {})
    headers.pop("x-opencode-request", None)
    return headers


def _fresh_identity_headers(base: Optional[Dict[str, str]]) -> Dict[str, str]:
    """Salinan headers dengan session ID yang sepenuhnya baru.

    HANYA untuk upaya terakhir fresh-session (semua target 403 termasuk
    direct): bila identitas lama yang di-flag upstream, identitas baru
    memberi peluang lolos. Format tetap valid ala CLI
    (`ses_` + 12 hex timestamp + 14 base62); header lain (client, project,
    User-Agent) dipertahankan agar fingerprint caller tetap konsisten.
    `x-opencode-request` DIHAPUS (gate v2: header itu = 403).
    JANGAN dipakai untuk retry biasa — sesi stabil per-percakapan adalah
    syarat cache affinity + issuance encrypted_content (lihat
    `_stable_opencode_session`).
    """
    headers = dict(base or {})
    fresh = _new_opencode_session_id()
    headers["x-opencode-session"] = fresh
    headers["x-opencode-session-id"] = fresh
    headers["x-session-affinity"] = fresh
    headers["x-session-id"] = fresh
    headers.pop("x-opencode-request", None)
    return headers


def _fresh_retry_targets(
    targets: Any, payload: Any
) -> Tuple[List[Tuple[str, Dict[str, str]]], str, str]:
    """Bangun ulang daftar target untuk fase fresh-session retry.

    Return (new_targets, fresh_session, fresh_key):
    - SEMUA target (rotasi penuh, bukan satu target terakhir) memakai SATU
      session baru yang sama — menguji hipotesis "sesi lama yang di-flag"
      di SETIAP egress IP, bukan cuma satu. Penandaan 403-cooldown relay
      hanya memengaruhi batch request BERIKUTNYA, bukan list ini.
    - prompt_cache_key payload ikut disegarkan (bila payload memang punya
      key) agar pasangan (session, key) tetap konsisten seperti percakapan
      baru yang alami. Pasangan (sesi acak + key lama) tidak pernah terjadi
      di alam dan dicurigai memperbesar peluang 403 lanjutan.
    - Kunci klien yang eksplisit ikut disegarkan demi konsistensi pasangan;
      turn klien berikutnya menurunkan pasangan stabil normal seperti biasa
      (derivasi per-request, tidak disimpan).
    - Format tetap valid: session `ses_` ala CLI, key 32 hex seperti
      fingerprint[:32] bawaan proxy.
    """
    fresh_session = _new_opencode_session_id()
    fresh_key = secrets.token_hex(16)
    new_targets: List[Tuple[str, Dict[str, str]]] = []
    try:
        items = list(targets or [])
    except TypeError:
        items = []
    for entry in items:
        try:
            # Dukung target 2-tuple lama (url, headers) maupun 3-tuple baru
            # (url, headers, proxy_url) dari lapisan proxy SOCKS/HTTP:
            # elemen proxy dipertahankan agar fase fresh tetap memakai egress
            # yang sama (hanya identitas sesi yang disegarkan).
            if isinstance(entry, (list, tuple)) and len(entry) == 3:
                url, headers, proxy_url = entry
            elif isinstance(entry, (list, tuple)) and len(entry) == 2:
                url, headers = entry
                proxy_url = None
            else:
                continue
        except (TypeError, ValueError):
            continue
        fresh_headers = dict(headers or {})
        fresh_headers["x-opencode-session"] = fresh_session
        fresh_headers["x-opencode-session-id"] = fresh_session
        fresh_headers["x-session-affinity"] = fresh_session
        fresh_headers["x-session-id"] = fresh_session
        fresh_headers.pop("x-opencode-request", None)
        if proxy_url:
            new_targets.append((url, fresh_headers, proxy_url))
        else:
            new_targets.append((url, fresh_headers))
    if isinstance(payload, dict) and isinstance(payload.get("prompt_cache_key"), str):
        payload["prompt_cache_key"] = fresh_key
    return new_targets, fresh_session, fresh_key


# ── Free-tier client fingerprint gates (gate v2, diverifikasi live 2026-10-14) ──
# Upstream /inference/v1/* menolak request free-tier dengan 403 FreeTierError
# bila salah satu gate tidak terpenuhi (live-bisect terhadap CLI v2.0.26):
#   1. Endpoint: /zen/v1/* MATI — semua free request di sana 403 blanket.
#   2. User-Agent bentuk `opencode/<channel>/<version>/<client>`
#      (mis. `opencode/latest/2.0.26/cli`).
#   3. Header sesi: `x-opencode-session` DAN `x-opencode-session-id`
#      (salah satu saja -> 403); `x-opencode-request` ADA NILAI -> 403.
#   4. Body: `stream:true` WAJIB (chat maupun responses).
#   5. Tools: definisi KANONIK OpenCode `read` + `shell` (deskripsi + schema
#      PERSIS) WAJIB hadir — stub (nama saja / parameters kosong / deskripsi
#      palsu) ditolak 403. Nama lain (bash/glob/grep/read stubs) sudah tidak
#      cukup.
# Bentuk tools berbeda per API: chat memakai bungkus
# {"type":"function","function":{...}}, responses memakai flat
# {"type":"function","name":...}. Klien (Hermes dsb.) jarang mengirim tools
# ini, jadi proxy menyuntikkannya agar terlihat seperti OpenCode CLI.
OPENCODE_FINGERPRINT_TOOLS = ("read", "shell")

# Definisi kanonik VERBATIM dari source resmi opencode v2.0.26
# (packages/core/src/tool/plugin/read.ts + shell.ts). Schema mengikuti
# Effect Schema -> JSON Schema: optional key tidak masuk `required`.
_OPENCODE_CANONICAL_TOOL_SPECS: Dict[str, Dict[str, Any]] = {
    "read": {
        "description": (
            "Read the contents of a file or directory. Supports text files, images, and PDFs. "
            "Images and PDFs are presented directly to the model. Each text line is prefixed by "
            "its 1-based line number as <line>: <content>. The prefix is for reference and is not "
            "part of the file content. Directory entries are returned one per line. Use offset and "
            "limit to read large files or directories in sections. Prefer one larger read over "
            "many small slices, and use grep to find specific content in large files."
        ),
        "properties": {
            "path": {
                "type": "string",
                "description": "File or directory to read",
            },
            "offset": {
                "type": "integer",
                "description": "The line or directory entry to start reading from (1-based)",
            },
            "limit": {
                "type": "integer",
                "description": "The maximum number of lines or directory entries to read (defaults to and capped at 2000)",
            },
        },
        "required": ["path"],
    },
    "shell": {
        "description": (
            "Execute a shell command and return its output. "
            "Quote file paths containing spaces or special characters. "
            "Prefer dedicated tools over shell commands when possible. "
            "When output is large, the full result is saved to a file and a truncated preview is returned. "
            "Rely on automatic truncation unless filtering the output is more useful. "
            "Commands accept an optional timeout, background commands have no timeout by default. "
            "Background commands return immediately, and you will be notified when they complete."
        ),
        "properties": {
            "command": {
                "type": "string",
                "description": "Shell command string to execute",
            },
            "workdir": {
                "type": "string",
                "description": (
                    "Working directory to execute the command in. Defaults to the current working "
                    "directory. When possible, avoid changing directories in the command and set "
                    "the working directory here instead."
                ),
            },
            "timeout": {
                "type": "integer",
                "description": (
                    "Timeout in milliseconds. Set to 0 to disable the timeout. Defaults to 120000 "
                    "for foreground commands. Background commands have no timeout by default."
                ),
            },
            "background": {
                "type": "boolean",
                "description": (
                    "Run the command in the background and return immediately (useful for dev "
                    "servers and long-running builds). You do not need to use '&' at the end of "
                    "your command when using this parameter. You will be notified when it "
                    "completes. DO NOT poll for completion."
                ),
            },
        },
        "required": ["command"],
    },
}

# Catatan sistem yang disisipkan saat tool kanonik read/shell disuntikkan
# (klien tidak mendeklarasikannya). TANPA catatan ini model benar-benar
# MEMANGGIL read/shell hasil suntikan (phantom tool calls). Redaksi PENTING
# (diverifikasi live): frasa "tools are not available" membuat model juga
# menolak tool milik klien sendiri (Read/Bash); "placeholders ... use the
# other tools you have been given" membuat tool klien tetap jalan (0 phantom).
_OPENCODE_PLACEHOLDER_TOOLS_NOTE = (
    "The read and shell tools in this conversation are placeholders provided for "
    "compatibility and cannot be executed. Use the other tools you have been "
    "given to complete the task."
)


def _canonical_chat_tool(name: str) -> Dict[str, Any]:
    """Definisi kanonik satu tool (read/shell) dalam bentuk chat (nested)."""
    spec = _OPENCODE_CANONICAL_TOOL_SPECS[name]
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": spec["description"],
            "parameters": {
                "type": "object",
                "properties": spec["properties"],
                "required": list(spec["required"]),
            },
        },
    }


def _canonical_responses_tool(name: str) -> Dict[str, Any]:
    """Definisi kanonik satu tool (read/shell) dalam bentuk responses (flat)."""
    spec = _OPENCODE_CANONICAL_TOOL_SPECS[name]
    return {
        "type": "function",
        "name": name,
        "description": spec["description"],
        "parameters": {
            "type": "object",
            "properties": spec["properties"],
            "required": list(spec["required"]),
        },
    }


def restore_client_tool_name(name: Any) -> Any:
    """Kembalikan nama tool klien yang di-alias gateway (read_client -> read).

    Gateway meng-alias tool klien yang namanya PERSIS `read`/`shell` menjadi
    `<name>_client` agar tidak bentrok dengan definisi kanonik yang disuntikkan.
    Pemulihan diterapkan di sisi respons agar penanganan tool di klien
    sepenuhnya transparan. Nilai lain diloloskan apa adanya.
    """
    if isinstance(name, str) and name in ("read_client", "shell_client"):
        return name[: -len("_client")]
    return name


def restore_aliased_names_in_response(obj: Any) -> Any:
    """Pulihkan nama tool ter-alias pada objek/event Responses (in-place).

    Menangani `output[].name` (objek response penuh) dan `item.name` (event
    `response.output_item.*`). Dipakai jalur passthrough responses mentah agar
    klien tidak melihat nama alias internal (`read_client`).
    """
    if not isinstance(obj, dict):
        return obj
    output = obj.get("output")
    if isinstance(output, list):
        for item in output:
            if isinstance(item, dict) and item.get("type") == "function_call":
                item["name"] = restore_client_tool_name(item.get("name"))
    item = obj.get("item")
    if isinstance(item, dict) and item.get("type") == "function_call":
        item["name"] = restore_client_tool_name(item.get("name"))
    return obj


def _alias_canonical_tool_conflicts_names(names: Any) -> "tuple[set, bool]":
    """Hitung nama tool yang bentrok persis dengan kanonik (read/shell)."""
    conflicts: set = set()
    if isinstance(names, (list, tuple, set)):
        for name in names:
            if isinstance(name, str) and name in OPENCODE_FINGERPRINT_TOOLS:
                conflicts.add(name)
    return conflicts, bool(conflicts)


def _prepend_placeholder_note(payload: Dict[str, Any], logger_tag: str) -> None:
    """Sisipkan catatan placeholder di paling depan system prompt payload.

    Chat: prepend `{"role":"system","content":NOTE}` ke `messages`.
    Responses: prepend item system ke `input` (bila input berupa list;
    bila string, input dibungkus jadi satu item user lebih dulu? TIDAK —
    string input dibiarkan, catatan masuk via item system di depan).
    Idempoten: tidak menambah bila catatan sudah ada.
    """
    if not isinstance(payload, dict):
        return
    note = _OPENCODE_PLACEHOLDER_TOOLS_NOTE
    messages = payload.get("messages")
    if isinstance(messages, list):
        for message in messages:
            if isinstance(message, dict) and message.get("role") == "system":
                content = message.get("content")
                if isinstance(content, str) and note in content:
                    return
        payload["messages"] = [{"role": "system", "content": note}] + messages
        return
    items = payload.get("input")
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict) and item.get("role") == "system":
                content = item.get("content")
                texts = []
                if isinstance(content, str):
                    texts.append(content)
                elif isinstance(content, list):
                    texts.extend(
                        part.get("text", "")
                        for part in content
                        if isinstance(part, dict) and isinstance(part.get("text"), str)
                    )
                if any(note in text for text in texts):
                    return
        payload["input"] = [{"role": "system", "content": note}] + items
        return
    if isinstance(items, str):
        payload["input"] = [
            {"role": "system", "content": note},
            {"role": "user", "content": items},
        ]
        return
    # Responses tanpa input sama sekali: sisipkan lewat `instructions`.
    instructions = payload.get("instructions")
    if isinstance(instructions, str):
        if note in instructions:
            return
        payload["instructions"] = f"{note}\n\n{instructions}"
    elif not instructions:
        payload["instructions"] = note

# Model yang HANYA dilayani lewat Responses API (substring, lowercase).
OPENCODE_RESPONSES_MODELS = frozenset({
    "muse-spark-1.2-contributor-free",
    "muse-spark-1.3-contributor-free",
})


def _tool_name_of(tool: Any) -> str:
    """Ambil nama tool dari bentuk chat, responses, maupun MCP (toleran)."""
    if not isinstance(tool, dict):
        return ""
    name = tool.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    function = tool.get("function")
    if isinstance(function, dict):
        fname = function.get("name")
        if isinstance(fname, str) and fname.strip():
            return fname.strip()
    return ""


def _tool_schema_of(tool: Any) -> Optional[Dict[str, Any]]:
    """Ambil JSON Schema tool dari `parameters` (OpenAI) atau `inputSchema` (MCP)."""
    if not isinstance(tool, dict):
        return None
    for key in ("parameters", "inputSchema", "input_schema"):
        schema = tool.get(key)
        if isinstance(schema, dict):
            return schema
    function = tool.get("function")
    if isinstance(function, dict):
        for key in ("parameters", "inputSchema", "input_schema"):
            schema = function.get(key)
            if isinstance(schema, dict):
                return schema
    return None


def normalize_chat_tools(tools: Any) -> Optional[List[Dict[str, Any]]]:
    """Normalisasi tools MCP/Responses/chat menjadi bentuk chat OpenAI.

    MCP clients (opencode.json `mcp` servers, Claude Desktop) mendeskripsikan
    schema sebagai `inputSchema`, sedangkan OpenAI memakai `parameters`.
    Upstream chat HANYA mengerti `{"type":"function","function":{...}}` dengan
    `parameters` — tanpa normalisasi, schema MCP hilang/ditolak. Fungsi ini:
    - `inputSchema`/`input_schema` -> `parameters` (kedua level: top & function)
    - flat Responses/MCP `{"name":...}` -> bungkus chat `{"type":"function","function":{...}}`
    - mempertahankan `description`, `parameters`, dan `strict` bila ada.
    Tidak pernah melempar; return None bila tidak ada tools valid.
    """
    if not isinstance(tools, list) or not tools:
        return None
    out: List[Dict[str, Any]] = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        function = tool.get("function") if isinstance(tool.get("function"), dict) else None
        if function is not None:
            name = function.get("name")
            if not isinstance(name, str) or not name.strip():
                continue
            schema = _tool_schema_of(tool)
            entry: Dict[str, Any] = {
                "type": "function",
                "function": {
                    "name": name.strip(),
                    "description": function.get("description") or tool.get("description") or f"Tool {name.strip()}",
                    "parameters": schema if schema is not None else {"type": "object", "properties": {}},
                },
            }
            if isinstance(function.get("strict"), bool):
                entry["function"]["strict"] = function["strict"]
            elif isinstance(tool.get("strict"), bool):
                entry["function"]["strict"] = tool["strict"]
            out.append(entry)
            continue
        name = tool.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        schema = _tool_schema_of(tool)
        entry = {
            "type": "function",
            "function": {
                "name": name.strip(),
                "description": tool.get("description") or f"Tool {name.strip()}",
                "parameters": schema if schema is not None else {"type": "object", "properties": {}},
            },
        }
        if isinstance(tool.get("strict"), bool):
            entry["function"]["strict"] = tool["strict"]
        out.append(entry)
    return out or None


# Upstream `/zen/v1/responses` WAJIB nama tool hanya `[a-zA-Z0-9_-]`
# (`^[a-zA-Z0-9_-]+$`). Tools MCP yang namanya mengandung `:` atau `.`
# (mis. `mcp:fs:read`, `server.getTime`) sebelumnya menyebabkan 400
# `invalid_request_error` di SEMUA target — request MCP gagal padahal
# request biasa lolos. Sanitasi: karakter di luar kelas itu -> `_`.
_RESPONSES_TOOL_NAME_INVALID = re.compile(r"[^a-zA-Z0-9_-]")


def sanitize_responses_tool_name(name: Any) -> str:
    """Nama tool yang layak `/zen/v1/responses` (`^[a-zA-Z0-9_-]+$`).

    Karakter di luar kelas diganti `_`; nama kosong menghasilkan `_`.
    Tidak pernah melempar.
    """
    if not isinstance(name, str):
        return "_"
    cleaned = _RESPONSES_TOOL_NAME_INVALID.sub("_", name.strip())
    return cleaned or "_"


def normalize_responses_tools(tools: Any) -> Optional[List[Dict[str, Any]]]:
    """Normalisasi tools chat/MCP menjadi bentuk flat Responses API.

    Kebalikan `normalize_chat_tools`: `{"type":"function","function":{...}}`
    (chat) maupun MCP `{"name":..., "inputSchema":...}` -> flat
    `{"type":"function","name":...,"parameters":...}`. `strict` dipertahankan.
    Nama tool disanitasi ke `^[a-zA-Z0-9_-]+$` (batas keras upstream
    `/zen/v1/responses`; MCP `mcp:fs:read` -> `mcp_fs_read`).
    """
    if not isinstance(tools, list) or not tools:
        return None
    out: List[Dict[str, Any]] = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        function = tool.get("function") if isinstance(tool.get("function"), dict) else None
        if function is not None:
            name = function.get("name")
            if not isinstance(name, str) or not name.strip():
                continue
            schema = _tool_schema_of(tool)
            entry: Dict[str, Any] = {
                "type": "function",
                "name": sanitize_responses_tool_name(name),
                "description": function.get("description") or tool.get("description") or f"Tool {name.strip()}",
                "parameters": schema if schema is not None else {"type": "object", "properties": {}},
            }
            if isinstance(function.get("strict"), bool):
                entry["strict"] = function["strict"]
            elif isinstance(tool.get("strict"), bool):
                entry["strict"] = tool["strict"]
            out.append(entry)
            continue
        name = tool.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        schema = _tool_schema_of(tool)
        entry = {
            "type": "function",
            "name": sanitize_responses_tool_name(name),
            "description": tool.get("description") or f"Tool {name.strip()}",
            "parameters": schema if schema is not None else {"type": "object", "properties": {}},
        }
        if isinstance(tool.get("strict"), bool):
            entry["strict"] = tool["strict"]
        out.append(entry)
    return out or None


def sanitize_responses_items(items: Any) -> Tuple[List[Dict[str, Any]], int]:
    """Pairing item `function_call` / `function_call_output` yang yatim.

    Claude Code melakukan `/compact` dan user meng-interrupt turn tool ->
    `body.input` bisa berisi `function_call` TANPA `function_call_output`
    pasangannya (atau sebaliknya). Upstream `/zen/v1/responses` menolak
    turn seperti itu dengan 400 `No tool output found for function call ...`
    di SEMUA egress (salah payload, bukan salah relay). Aturan:
    - `function_call_output` yatim (tak ada call-nya) -> DIBUANG.
    - `function_call` yatim (tak ada output) -> dipasangkan dengan output
      sintetis `{"type":"function_call_output","call_id":...,"output":"{}"}`
      tepat setelah call-nya.

    Return (items_baru, n_perbaikan). Tak pernah melempar; input non-list
    dikembalikan apa adanya dengan n=0.
    """
    try:
        if not isinstance(items, list):
            return items, 0
        call_ids: set = set()
        output_ids: set = set()
        for item in items:
            if not isinstance(item, dict):
                continue
            cid = item.get("call_id")
            if not isinstance(cid, str) or not cid:
                continue
            if item.get("type") == "function_call":
                call_ids.add(cid)
            elif item.get("type") == "function_call_output":
                output_ids.add(cid)
        out: List[Dict[str, Any]] = []
        fixed = 0
        for item in items:
            if isinstance(item, dict) and item.get("type") == "function_call_output":
                cid = item.get("call_id")
                if not (isinstance(cid, str) and cid in call_ids):
                    fixed += 1
                    continue
            out.append(item)
            if isinstance(item, dict) and item.get("type") == "function_call":
                cid = item.get("call_id")
                if isinstance(cid, str) and cid and cid not in output_ids:
                    out.append({
                        "type": "function_call_output",
                        "call_id": cid,
                        "output": "{}",
                    })
                    output_ids.add(cid)
                    fixed += 1
        return out, fixed
    except (TypeError, ValueError, AttributeError):
        return (items if isinstance(items, list) else []), 0


# Batas keras `max_output_tokens` upstream /zen/v1/responses:
# - < 16 -> 400 "max_output_tokens must be >= 16" (probe 1-token kuota/
#   health dari jalur MCP opencode).
# - > 65536 -> 400 "The request contains invalid parameters".
# - input_tokens + max_output_tokens > 1.048.576 (combo context 1M) ->
#   400 "The request contains invalid parameters".
RESPONSES_MIN_OUTPUT_TOKENS = 16
RESPONSES_MAX_OUTPUT_TOKENS = 65536
SPARK_MAX_TOTAL_TOKENS = 1_048_576


def estimate_prompt_tokens(payload: Any) -> int:
    """Estimasi kasar token input (chars//4 + media + tools).

    Dipakai untuk dua hal: guard combo 1M upstream dan clamp output per
    model. Sengaja konservatif (lebih banyak estimasi = clamp lebih dini
    = aman): teks len//4, tiap part image/file +1000, definisi tools
    (nama/deskripsi/schema JSON) ikut dihitung karena MCP-heavy request
    bisa puluhan ribu token tanpa satu pun pesan panjang. Bukan pengganti
    tokenizer; hanya guard overflow, bukan untuk billing. Tak pernah
    melempar.
    """
    try:
        total_chars = 0
        images = 0

        def _add_text(value: Any) -> None:
            nonlocal total_chars
            if isinstance(value, str):
                total_chars += len(value)

        def _add_json(value: Any) -> None:
            nonlocal total_chars
            if value is None:
                return
            try:
                total_chars += len(
                    json.dumps(value, ensure_ascii=False, separators=(",", ":"))
                )
            except (TypeError, ValueError):
                _add_text(str(value))

        def _walk_content(content: Any) -> None:
            nonlocal images
            if isinstance(content, str):
                _add_text(content)
                return
            if not isinstance(content, list):
                return
            for part in content:
                if isinstance(part, str):
                    _add_text(part)
                elif isinstance(part, dict):
                    ptype = part.get("type")
                    if ptype in ("input_image", "image_url", "input_file", "file"):
                        images += 1
                    for key in ("text", "delta"):
                        _add_text(part.get(key))
                    if ptype == "function_call":
                        _add_text(part.get("arguments"))

        for key in ("instructions", "system"):
            _add_text(payload.get(key))
        items = payload.get("input")
        if isinstance(items, str):
            _add_text(items)
        elif isinstance(items, list):
            for item in items:
                if isinstance(item, str):
                    _add_text(item)
                elif isinstance(item, dict):
                    if item.get("type") in ("function_call", "function_call_output"):
                        _add_text(item.get("arguments"))
                        _add_text(item.get("output"))
                        continue
                    _walk_content(item.get("content"))
        messages = payload.get("messages")
        if isinstance(messages, list):
            for message in messages:
                if isinstance(message, dict):
                    _walk_content(message.get("content"))
        # Definisi tools (chat `function.parameters` maupun Responses/MCP
        # `parameters`/`inputSchema`) tidak pernah lewat `messages`, jadi
        # tanpa ini request ber-tools besar dianggap kecil.
        tools = payload.get("tools")
        if isinstance(tools, list):
            for tool in tools:
                if isinstance(tool, str):
                    _add_text(tool)
                elif isinstance(tool, dict):
                    _add_json(tool)
        return total_chars // 4 + images * 1000
    except (TypeError, ValueError, AttributeError):
        return 0


# Tokenizer asli tidak tersedia (tanpa dependensi baru); chars//4 bisa
# UNDER-estimate pada JSON schema, kode, dan teks non-Latin. Faktor + margin
# ini membuat clamp turun lebih awal agar upstream tidak balas 400
# "context length" tepat di batas.
CONTEXT_INPUT_SAFETY_FACTOR = 1.10
CONTEXT_INPUT_SAFETY_TOKENS = 512


def _model_output_budget(window: int, payload: Any, minimum: int) -> Optional[int]:
    """Sisa token output yang aman untuk `window` setelah estimasi input."""
    estimated = estimate_prompt_tokens(payload)
    if estimated <= 0:
        return None
    effective = int(estimated * CONTEXT_INPUT_SAFETY_FACTOR) + CONTEXT_INPUT_SAFETY_TOKENS
    return max(minimum, window - effective)


def clamp_max_tokens_for_model(
    value: Any,
    payload: Any = None,
    model_id: Optional[str] = None,
    *,
    minimum: int = 1,
) -> Optional[int]:
    """Turunkan `max_tokens`/output agar input + output muat window model.

    Schema chat memakai default `max_tokens=65536`; untuk model ber-window
    131K (deepseek-v4*) itu menyisakan ~65K input saja bila upstream
    menghitung prompt+output sebagai satu context. Fungsi ini menurunkan
    nilai HANYA bila perlu: tidak pernah menaikkan permintaan klien, dan
    mengembalikan None untuk nilai kosong/invalid agar default lama tetap
    berlaku. Tak pernah melempar.
    """
    try:
        if value is None or isinstance(value, bool):
            return None
        v = int(value)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    model = model_id
    if not model and isinstance(payload, dict):
        model = payload.get("model")
    if not model:
        return v
    try:
        window = int(get_model_context_window(model))
        budget = _model_output_budget(window, payload, minimum)
    except (TypeError, ValueError, AttributeError):
        return v
    if budget is not None and v > budget:
        return budget
    return v


def clamp_responses_max_output_tokens(value: int, payload: Any = None) -> int:
    """Clamp `max_output_tokens` ke [16, 65536] + guard total 1M.

    Return nilai yang aman dikirim ke upstream /zen/v1/responses. Bila
    `payload` diberikan, estimasi token input (termasuk tools) dipakai
    untuk mencegah `input_tokens + max_output_tokens` melewati combo 1M
    upstream MAUPUN window model itu sendiri. Tak pernah melempar.
    """
    try:
        v = int(value)
    except (TypeError, ValueError):
        return RESPONSES_MAX_OUTPUT_TOKENS
    if v < RESPONSES_MIN_OUTPUT_TOKENS:
        v = RESPONSES_MIN_OUTPUT_TOKENS
    if v > RESPONSES_MAX_OUTPUT_TOKENS:
        v = RESPONSES_MAX_OUTPUT_TOKENS
    if isinstance(payload, dict):
        estimated = estimate_prompt_tokens(payload)
        if estimated > 0 and estimated + v > SPARK_MAX_TOTAL_TOKENS:
            v = max(RESPONSES_MIN_OUTPUT_TOKENS, SPARK_MAX_TOTAL_TOKENS - estimated)
        # Batas combo 1M adalah batas API, bukan batas model: gpt-5 (400K),
        # claude (1M), deepseek (131K) tetap menolak di window masing-masing.
        # Pakai budget per model bila id-nya dikenal; payload tanpa model
        # (unit test / probe) tetap hanya memakai guard 1M.
        model_id = payload.get("model")
        if model_id:
            try:
                window = min(SPARK_MAX_TOTAL_TOKENS, int(get_model_context_window(model_id)))
                budget = _model_output_budget(window, payload, RESPONSES_MIN_OUTPUT_TOKENS)
            except (TypeError, ValueError, AttributeError):
                budget = None
            if budget is not None and v > budget:
                v = budget
    return v


# ── Normalisasi alias parameter tool Claude Code ──
# Model non-Anthropic yang dipakai lewat Claude Code CLI / Claude Desktop
# (oc/muse-spark-*, glm-*, qwen-*, deepseek-*) kerap menghalusinasi nama
# parameter saat memanggil tool built-in Claude:
#   InputValidationError: Bash failed due to the following issue:
#   The required parameter `command` is missing
# Pemetaan di bawah hanya berlaku bila kanonik TIDAK ada (alias dipindah,
# bukan diduplikasi). Nama tool dicocokkan case-insensitive.
_CLAUDE_TOOL_INPUT_ALIASES: Dict[str, Dict[str, Tuple[str, ...]]] = {
    "bash": {
        "command": ("cmd", "script", "command_line", "commandLine", "input", "code"),
    },
    "read": {
        "file_path": ("path", "file", "filepath", "filePath", "filename"),
    },
    "edit": {
        "file_path": ("path", "file", "filepath", "filePath", "filename"),
        "old_string": ("old_text",),
        "new_string": ("new_text",),
    },
    "write": {
        "file_path": ("path", "file", "filepath", "filePath", "filename"),
        "content": ("text",),
    },
    "multiedit": {
        "file_path": ("path", "file", "filepath", "filePath", "filename"),
    },
    "glob": {
        "pattern": ("query", "search", "regex", "glob"),
        "path": ("dir", "directory"),
    },
    "grep": {
        "pattern": ("query", "search", "regex", "glob"),
        "path": ("dir", "directory"),
    },
}


def normalize_claude_tool_input(tool_name: Any, args: Any) -> Any:
    """Normalkan alias parameter tool Claude Code ke nama kanonik.

    `args` boleh dict atau string JSON (kembalikan dengan tipe yang sama).
    Hanya alias->kanonik yang tercatat untuk tool tersebut; kanonik yang
    sudah ada selalu menang. Tool tak dikenal / JSON rusak -> input
    dikembalikan apa adanya. Tak pernah melempar.
    """
    if not isinstance(tool_name, str):
        return args
    table = _CLAUDE_TOOL_INPUT_ALIASES.get(tool_name.strip().lower())
    if not table:
        return args
    is_str = isinstance(args, str)
    if is_str:
        try:
            parsed = json.loads(args)
        except (ValueError, TypeError):
            return args
    elif isinstance(args, dict):
        parsed = args
    else:
        return args
    if not isinstance(parsed, dict):
        return args
    out = dict(parsed)
    changed = False
    for canonical, aliases in table.items():
        if canonical in out:
            continue
        for alias in aliases:
            if alias in out:
                out[canonical] = out.pop(alias)
                changed = True
                break
    if not changed:
        return args
    if is_str:
        try:
            return json.dumps(out, ensure_ascii=False)
        except (TypeError, ValueError):
            return args
    return out


def ensure_chat_fingerprint_tools(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Suntik tool kanonik read/shell (bentuk chat) + alias konflik + note.

    Urutan:
    1. Normalisasi tools MCP/Responses (idempotent) -> bentuk chat.
    2. Alias: tool klien yang namanya PERSIS `read`/`shell` di-rename jadi
       `<name>_client` (di `tools` DAN di `messages[].tool_calls[].function.
       name` pada histori assistant) agar tidak bentrok dengan definisi
       kanonik yang disuntikkan — upstream menolak tool nama sama, definisi beda.
    3. Suntik definisi kanonik `read` + `shell` (deskripsi + schema PERSIS
       v2.0.26) — gate upstream /inference mewajibkan keduanya.
    4. Placeholder system note (agar model tidak memanggil read/shell suntikan
       dan tidak menolak tool milik klien sendiri).
    """
    if not isinstance(payload, dict):
        return payload
    tools = payload.get("tools")
    # Normalisasi dulu (idempotent): tools MCP/Responses (`inputSchema`,
    # flat `name`) -> bentuk chat agar skema tidak hilang di wire.
    if isinstance(tools, list) and tools:
        try:
            _norm = normalize_chat_tools(tools)
            if _norm is not None:
                payload["tools"] = tools = _norm
        except (AttributeError, TypeError, ValueError):
            pass
    if not isinstance(tools, list):
        tools = []
        payload["tools"] = tools
    present = {_tool_name_of(t) for t in tools}
    conflicts, _ = _alias_canonical_tool_conflicts_names(present)
    appended = False
    if conflicts:
        # Alias tool klien bentrok -> <name>_client (tools + histori pesan).
        for tool in tools:
            if not isinstance(tool, dict):
                continue
            function = tool.get("function")
            target = function if isinstance(function, dict) else tool
            if target.get("name") in conflicts:
                target["name"] = f"{target['name']}_client"
        messages = payload.get("messages")
        if isinstance(messages, list):
            for message in messages:
                if not isinstance(message, dict):
                    continue
                tool_calls = message.get("tool_calls")
                if not isinstance(tool_calls, list):
                    continue
                for call in tool_calls:
                    if not isinstance(call, dict):
                        continue
                    function = call.get("function")
                    if isinstance(function, dict) and function.get("name") in conflicts:
                        function["name"] = f"{function['name']}_client"
    for name in OPENCODE_FINGERPRINT_TOOLS:
        if name in present and name not in conflicts:
            continue
        tools.append(_canonical_chat_tool(name))
        appended = True
    if appended:
        _prepend_placeholder_note(payload, "chat")
    # Tools MCP kerap membawa $ref siklik (skema rekursif) yang ditolak
    # provider Console dengan 400 di SEMUA target — putus siklusnya di sini
    # (satu choke point untuk SEMUA jalur chat) sebelum wire.
    try:
        _, _n_fixed = sanitize_tools_for_upstream(tools)
        if _n_fixed:
            try:
                from app.core.logging_utils import _log as _san_log
                _san_log("TOOLS", f"sanitized {_n_fixed} recursive tool schema(s) (chat)")
            except (ImportError, AttributeError, TypeError, ValueError):
                pass
    except (AttributeError, TypeError, ValueError):
        pass
    return payload


def ensure_responses_fingerprint_tools(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Suntik tool kanonik read/shell (bentuk responses) + alias + note.

    Versi Responses dari `ensure_chat_fingerprint_tools`: bentuk tools flat,
    alias konflik diterapkan pada `tools` maupun item `function_call` di
    `input` (name di-rename, call_id dipertahankan).
    """
    if not isinstance(payload, dict):
        return payload
    tools = payload.get("tools")
    # Normalisasi dulu (idempotent): tools MCP (`inputSchema`) maupun bentuk
    # chat (`function.{...}`) -> flat responses. TANPA ini, tools MCP yang
    # masuk via /v1/responses direct wire tanpa `parameters` (skema hilang)
    # dan berisiko 400/403 upstream — padahal "biasa" (tanpa MCP) lolos.
    if isinstance(tools, list) and tools:
        try:
            _norm = normalize_responses_tools(tools)
            if _norm is not None:
                payload["tools"] = tools = _norm
        except (AttributeError, TypeError, ValueError):
            pass
    if not isinstance(tools, list):
        tools = []
        payload["tools"] = tools
    present = {_tool_name_of(t) for t in tools}
    conflicts, _ = _alias_canonical_tool_conflicts_names(present)
    appended = False
    if conflicts:
        # Alias tool klien bentrok -> <name>_client (tools + item input).
        for tool in tools:
            if isinstance(tool, dict) and tool.get("name") in conflicts:
                tool["name"] = f"{tool['name']}_client"
        items = payload.get("input")
        if isinstance(items, list):
            for item in items:
                if (
                    isinstance(item, dict)
                    and item.get("type") == "function_call"
                    and item.get("name") in conflicts
                ):
                    item["name"] = f"{item['name']}_client"
    for name in OPENCODE_FINGERPRINT_TOOLS:
        if name in present and name not in conflicts:
            continue
        tools.append(_canonical_responses_tool(name))
        appended = True
    if appended:
        _prepend_placeholder_note(payload, "responses")
    # Lihat ensure_chat_fingerprint_tools: putus skema rekursif ($ref siklik
    # MCP) yang ditolak provider dengan 400 di semua target.
    try:
        _, _n_fixed = sanitize_tools_for_upstream(tools)
        if _n_fixed:
            try:
                from app.core.logging_utils import _log as _san_log
                _san_log("TOOLS", f"sanitized {_n_fixed} recursive tool schema(s) (responses)")
            except (ImportError, AttributeError, TypeError, ValueError):
                pass
    except (AttributeError, TypeError, ValueError):
        pass
    return payload


SPARK_REASONING_EFFORT = "xhigh"


def ensure_spark_reasoning_xhigh(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Paksa reasoning effort xhigh untuk model muse-spark (Responses-only).

    Berlaku untuk SEMUA jalur (/v1/responses langsung maupun chat bridge):
    nilai klien (low/medium/high/hilang) selalu di-override. Model lain
    tidak disentuh.
    """
    if not isinstance(payload, dict):
        return payload
    try:
        from app.core.config import _is_responses_only_model
        is_spark = _is_responses_only_model(str(payload.get("model") or ""))
    except (ImportError, AttributeError, TypeError, ValueError):
        is_spark = False
    if not is_spark:
        return payload
    existing = payload.get("reasoning")
    if isinstance(existing, dict):
        existing = dict(existing)
        existing["effort"] = SPARK_REASONING_EFFORT
        payload["reasoning"] = existing
    else:
        payload["reasoning"] = {"effort": SPARK_REASONING_EFFORT}
    return payload


def ensure_responses_wire_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Normalisasi field Responses API ala CLI sebelum dikirim upstream.

    - max_tokens / max_completion_tokens (gaya chat) dipetakan ke
      max_output_tokens bila yang terakhir belum ada.
    - store=false (stateless; thinking tidak dipertahankan server).
    - Kuartet tools fingerprint disuntik.
    - muse-spark: reasoning effort selalu xhigh (override klien).
    """
    if not isinstance(payload, dict):
        return payload
    if payload.get("max_output_tokens") is None:
        for key in ("max_completion_tokens", "max_tokens"):
            value = payload.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                payload["max_output_tokens"] = int(value)
                break
    payload.pop("max_tokens", None)
    payload.pop("max_completion_tokens", None)
    # §8: max_output_tokens WAJIB ada (default proxy 65536 bila klien kirim
    # null/hilang — jalur MCP opencode kadang tidak mengirimnya, dan tanpa
    # ini upstream menjawab 403 walau fingerprint lain lengkap).
    _mo = payload.get("max_output_tokens")
    if not isinstance(_mo, (int, float)) or isinstance(_mo, bool):
        payload["max_output_tokens"] = 65536
    else:
        # Batas keras upstream: <16 (probe 1-token kuota/health) DAN
        # >65576->clamp; plus guard combo 1M context
        # (input + output > 1.048.576 -> 400 invalid parameters).
        payload["max_output_tokens"] = clamp_responses_max_output_tokens(
            int(_mo), payload
        )
    # Null eksplisit (temperature:null dsb. dari klien MCP) bila diteruskan
    # mentah berisiko 400 upstream — drop agar upstream pakai default-nya.
    for _opt in ("temperature", "top_p"):
        if _opt in payload and payload.get(_opt) is None:
            payload.pop(_opt, None)
    payload["store"] = False
    ensure_responses_fingerprint_tools(payload)
    ensure_spark_reasoning_xhigh(payload)
    coerce_tool_choice_auto(payload, "responses-wire")
    # Pairing item function_call/function_call_output yatim (Claude Code
    # /compact atau interrupt tool turn) — upstream 400 "No tool output
    # found for function call ..." bila ada pasangan yang putus.
    _input_items = payload.get("input")
    if isinstance(_input_items, list):
        _items, _n_paired = sanitize_responses_items(_input_items)
        if _n_paired:
            payload["input"] = _items
            try:
                from app.core.logging_utils import _log as _pair_log
                _pair_log(
                    "RESP",
                    f"paired/dropped {_n_paired} orphan function_call/"
                    f"function_call_output item(s) (responses-wire)",
                )
            except (ImportError, AttributeError, TypeError, ValueError):
                pass
    # prompt_cache_key stabil per-percakapan (bagian fingerprint gate §8):
    # jalur bridge mensintesisnya, jalur direct passthrough selama ini tidak —
    # samakan agar wire direct selengkap wire bridge yang terbukti lolos.
    if not payload.get("prompt_cache_key"):
        try:
            _fp = _conversation_fingerprint(payload)
            if _fp:
                payload["prompt_cache_key"] = _fp[:32]
        except (TypeError, ValueError, AttributeError):
            pass
    return payload


def coerce_tool_choice_auto(payload: Dict[str, Any], context: str = "") -> bool:
    """Paksa tool_choice ke bentuk yang didukung provider Console.

    Upstream (Console) HANYA mendukung `"auto"` — `none`/`required`/named
    function choice ditolak 400 `invalid_request_error` (live 2026-09-24:
    Kilo Code mengirim named/required ke spark & mimo -> 400 -> 502 ke
    klien di SEMUA target karena ini salah payload, bukan salah route).
    Aturan: tools ada dan choice bukan auto -> `"auto"`; tools tidak ada ->
    key di-drop (choice tanpa tools tak bermakna). Return True bila payload
    diubah (sekaligus dilog), False bila sudah benar. Tidak pernah melempar.
    Berlaku di SEMUA jalur payload upstream (chat, bridge, responses,
    reverse bridge) karena batasan ini milik provider, bukan model.
    """
    try:
        if not isinstance(payload, dict):
            return False
        tools = payload.get("tools")
        has_tools = isinstance(tools, list) and len(tools) > 0
        current = payload.get("tool_choice", None)
        if has_tools:
            if current == "auto":
                return False
            payload["tool_choice"] = "auto"
        else:
            if "tool_choice" not in payload:
                return False
            payload.pop("tool_choice", None)
        try:
            from app.core.logging_utils import _log as _tools_log
            _tools_log(
                "TOOLS",
                f"tool_choice {current!r} -> "
                f"{payload.get('tool_choice', '<dropped>')!r} "
                f"({context or 'unspecified'})",
            )
        except (ImportError, AttributeError, TypeError, ValueError):
            pass
        return True
    except (AttributeError, TypeError, ValueError):
        return False


# ── Sanitizer skema JSON rekursif (tools MCP) ──
# Provider Console menolak skema rekursif dengan 400
# "Recursive JSON schemas are not currently supported" (live: muse-spark via
# /v1/responses dengan tools MCP bersiklus $ref -> 400 di SEMUA target karena
# ini salah payload, bukan salah route). Tools MCP (filesystem, opencode
# built-in, dsb.) lumrah membawa $ref siklik gaya Pydantic
# (`#/$defs/Node` -> ... -> `#/$defs/Node`, termasuk mutual A<->B), jadi
# gateway memutus siklusnya sebelum wire: $ref yang membentuk back-edge
# (target satu SCC siklik dengan frame ekspansi aktif) diganti placeholder
# `{"type": "object"}`. $ref non-siklik dipertahankan apa adanya (tidak
# di-inline) agar payload tetap kecil dan skema utuh.
_RECURSION_PLACEHOLDER_DESC = (
    "Truncated by gateway: provider rejects recursive JSON schemas"
)

_DEPTH_PLACEHOLDER_DESC = (
    "Truncated by gateway: provider allows max 10 schema nesting levels"
)

_SCHEMA_MAX_DEPTH = 40

# Batas nesting provider Console: 400 "JSON schema exceeds the maximum
# nesting depth of 10 levels" (live muse-spark + tools MCP dalam, yang skema
# mentahnya >10 level). Dihitung 0-indexed per level JSON (dict maupun list):
# di atas 7 (total 9 level) dipadatkan — strictly di bawah 10 apa pun basis
# hitung provider (root dihitung/tidak), dengan tetap mempertahankan `type`
# node asal agar validasi longgar tapi benar arah (bukan object buta).
# Skema tulisan-tangan (2-4 level) tak tersentuh; hanya monster auto-generate
# yang dipadatkan.
_PROVIDER_MAX_SCHEMA_DEPTH = 7

_SCALAR_TYPES = frozenset({"string", "number", "integer", "boolean", "array", "object", "null"})


def _pointer_escape(segment: str) -> str:
    return str(segment).replace("~", "~0").replace("/", "~1")


def _pointer_of(path: tuple) -> str:
    return "#/" + "/".join(_pointer_escape(s) for s in path)


def _resolve_schema_pointer(root: Any, ref: str) -> Any:
    """Resolve JSON pointer internal (`#/$defs/X`, `#/definitions/X`, ...).

    Return node target atau None bila tak dapat di-resolve (ref eksternal /
    pointer rusak dibiarkan apa adanya). Tak pernah melempar.
    """
    try:
        if not isinstance(ref, str) or not ref.startswith("#"):
            return None
        if ref in ("#", "#/"):
            return root
        if not ref.startswith("#/"):
            return None
        node = root
        for raw_part in ref[2:].split("/"):
            part = raw_part.replace("~1", "/").replace("~0", "~")
            if isinstance(node, dict) and part in node:
                node = node[part]
            elif isinstance(node, list):
                try:
                    node = node[int(part)]
                except (ValueError, IndexError, TypeError):
                    return None
            else:
                return None
        return node
    except (TypeError, ValueError, AttributeError):
        return None


def _index_schema_defs(node: Any, path: tuple, defs: Dict[str, Any], seen: frozenset) -> None:
    """Index semua blok `$defs`/`definitions`: pointer-penuh -> node. Tak melempar."""
    try:
        if isinstance(node, list):
            for i, item in enumerate(node):
                _index_schema_defs(item, path + (str(i),), defs, seen)
            return
        if not isinstance(node, dict) or id(node) in seen:
            return
        seen = seen | {id(node)}
        for key, value in node.items():
            if key in ("$defs", "definitions") and isinstance(value, dict):
                for name, child in value.items():
                    try:
                        defs[_pointer_of(path + (key, str(name)))] = child
                    except (TypeError, ValueError):
                        continue
            _index_schema_defs(value, path + (key,), defs, seen)
    except (TypeError, ValueError, AttributeError, RecursionError):
        pass


def _collect_internal_refs(node: Any, out: set, seen: frozenset) -> None:
    """Kumpulkan SEMUA string `$ref` internal (`#...`) di subtree. Tak melempar."""
    try:
        if isinstance(node, list):
            for item in node:
                _collect_internal_refs(item, out, seen)
            return
        if not isinstance(node, dict) or id(node) in seen:
            return
        seen = seen | {id(node)}
        ref = node.get("$ref")
        if isinstance(ref, str) and ref.startswith("#"):
            out.add(ref)
        for value in node.values():
            _collect_internal_refs(value, out, seen)
    except (TypeError, ValueError, AttributeError, RecursionError):
        pass


def _cyclic_scc_map(defs: Dict[str, Any]) -> Tuple[Dict[str, frozenset], set]:
    """Petakan pointer definisi -> SCC, plus himpunan SCC yang siklik.

    SCC siklik = anggota >1, atau self-loop. $ref ke anggota SCC siklik
    HANYA diputus bila terjadi sebagai back-edge (ada frame ekspansi aktif
    dari SCC yang sama); tepi masuk dari luar dipertahankan agar bentuk
    skema level-atas tetap utuh. Tak pernah melempar.
    """
    try:
        edges: Dict[str, set] = {}
        for ptr, sub in defs.items():
            refs: set = set()
            _collect_internal_refs(sub, refs, frozenset())
            edges[ptr] = {q for q in refs if q in defs}
        reach: Dict[str, set] = {}
        for ptr in defs:
            seen: set = set()
            stack = list(edges.get(ptr, ()))
            while stack:
                cur = stack.pop()
                for nxt in edges.get(cur, ()):
                    if nxt not in seen:
                        seen.add(nxt)
                        stack.append(nxt)
            reach[ptr] = seen
        scc_of: Dict[str, frozenset] = {}
        cyclic: set = set()
        for ptr in defs:
            members = frozenset({ptr} | {
                q for q in defs
                if ptr in reach.get(q, set()) and q in reach.get(ptr, set())
            })
            scc_of[ptr] = members
            if len(members) > 1 or ptr in reach.get(ptr, set()):
                cyclic.add(members)
        return scc_of, cyclic
    except (TypeError, ValueError, AttributeError):
        return {}, set()


def _ensure_container_type(out: Any) -> Any:
    """Isi `type` yang hilang tanpa mengubah makna: node ber-`properties`
    adalah object, node ber-`items` adalah array menurut semantik JSON
    Schema. Validator ketat kadang menolak node tanpa `type`. Tak melempar."""
    try:
        if isinstance(out, dict) and "type" not in out:
            if isinstance(out.get("properties"), dict):
                out["type"] = "object"
            elif "items" in out:
                out["type"] = "array"
    except (TypeError, ValueError, AttributeError):
        pass
    return out


def _clean_schema_node(
    node: Any,
    root: Any,
    path: tuple,
    ref_stack: tuple,
    active: frozenset,
    depth: int,
    scc_of: Optional[Dict[str, frozenset]],
    cyclic_ids: set,
) -> Any:
    """Salinan node skema dengan back-edge $ref rekursif diputus. Tak melempar.

    `path` = lokasi tree saat ini (untuk pointer frame `$defs` akurat);
    `ref_stack` = pointer yang sedang diekspansi di jalur ini; `active` =
    id() dict ancestor (backstop siklus identitas); `depth` memutus nesting
    patologis.
    """
    try:
        if isinstance(node, list):
            if depth > _SCHEMA_MAX_DEPTH:
                return []
            if depth > _PROVIDER_MAX_SCHEMA_DEPTH:
                # Di batas provider: skalar (enum/required/type-array) tidak
                # menambah nesting bermakna -> pertahankan; komposisi ber-dict
                # (anyOf/oneOf/allOf/prefixItems dalam) dipadatkan satu opsi
                # object longgar (valid, bukan never-valid seperti [] kosong).
                if all(not isinstance(item, (dict, list)) for item in node):
                    return list(node)
                return [{"type": "object", "description": _DEPTH_PLACEHOLDER_DESC}]
            return [
                _clean_schema_node(item, root, path + (str(i),), ref_stack, active, depth + 1, scc_of, cyclic_ids)
                for i, item in enumerate(node)
            ]
        if not isinstance(node, dict):
            return node
        if id(node) in active or depth > _SCHEMA_MAX_DEPTH:
            return {"type": "object", "description": _RECURSION_PLACEHOLDER_DESC}
        active = active | {id(node)}
        if "additionalProperties" in node or node.get("required") == []:
            # Provider Console 400 "Invalid JSON schema" untuk
            # `additionalProperties` dalam BENTUK APA PUN (bool `false`
            # maupun dict ber-skema seperti `{"type": "object", ...}` —
            # umum di tools MCP auto-generate; live: `analysis_profile`
            # dkk. via spark DITOLAK walau bentuk dict).
            # Dilonggarkan (drop key) agar skema lolos: `required` asli yang
            # non-kosong dipertahankan (opsional tetap opsional — model
            # tidak dipaksa mengarang nilai), dan model jarang mengarang
            # key baru. `required: []` kosong ikut di-drop (tanpa makna,
            # sebagian validator menolaknya).
            node = {k: v for k, v in node.items()
                    if k != "additionalProperties" and not (k == "required" and v == [])}
        if depth > _PROVIDER_MAX_SCHEMA_DEPTH:
            # Padatkan: pertahankan type asal bila skalar-valid agar
            # array/string/number dalam tidak berubah jadi object.
            _t = node.get("type")
            _t = _t if isinstance(_t, str) and _t in _SCALAR_TYPES else "object"
            _d = node.get("description")
            _desc = (_d.strip() + " (deep schema truncated by gateway)") \
                if isinstance(_d, str) and _d.strip() else _DEPTH_PLACEHOLDER_DESC
            return {"type": _t, "description": _desc}
        active = active | {id(node)}
        ref = node.get("$ref")
        if isinstance(ref, str) and ref.startswith("#"):
            target = _resolve_schema_pointer(root, ref)
            is_back_edge = ref in ref_stack
            if not is_back_edge and target is not None and scc_of:
                try:
                    scc = scc_of.get(ref)
                    is_back_edge = (
                        scc is not None
                        and scc in cyclic_ids
                        and any(scc_of.get(p) == scc for p in ref_stack)
                    )
                except (TypeError, ValueError, AttributeError):
                    is_back_edge = False
            if target is not None and is_back_edge:
                desc = node.get("description")
                if isinstance(desc, str) and desc.strip():
                    return {
                        "type": "object",
                        "description": desc.strip() + " (recursive reference truncated by gateway)",
                    }
                return {"type": "object", "description": _RECURSION_PLACEHOLDER_DESC}
            child_stack = ref_stack + (ref,) if target is not None else ref_stack
            out: Dict[str, Any] = {}
            for key, value in node.items():
                if key == "$ref":
                    out[key] = value
                else:
                    out[key] = _clean_schema_node(
                        value, root, path + (key,), child_stack, active, depth + 1, scc_of, cyclic_ids
                    )
            return _ensure_container_type(out)
        # Blok definisi bernama = frame ekspansi (agar siklus terdeteksi
        # walau situs definisi sendiri bukan $ref).
        out = {}
        for key, value in node.items():
            if key in ("$defs", "definitions") and isinstance(value, dict):
                cleaned_defs = {}
                for def_name, def_node in value.items():
                    try:
                        ptr = _pointer_of(path + (key, str(def_name)))
                    except (TypeError, ValueError):
                        ptr = ""
                    cleaned_defs[def_name] = _clean_schema_node(
                        def_node, root, path + (key, str(def_name)),
                        ref_stack + ((ptr,) if ptr else ()),
                        active, depth + 1, scc_of, cyclic_ids,
                    )
                out[key] = cleaned_defs
            else:
                out[key] = _clean_schema_node(
                    value, root, path + (key,), ref_stack, active, depth + 1, scc_of, cyclic_ids
                )
        return _ensure_container_type(out)
    except (TypeError, ValueError, AttributeError, RecursionError):
        return {"type": "object", "description": _RECURSION_PLACEHOLDER_DESC}


def sanitize_tools_for_upstream(tools: Any) -> Tuple[List[Dict[str, Any]], int]:
    """Normalisasi skema tools ke subset aman provider (chat/responses/MCP).

    Menangani `parameters` maupun `inputSchema`/`input_schema` di level
    `function` maupun top-level: putus $ref siklik, padatkan nesting >batas,
    drop `additionalProperties` (bentuk apa pun) + `required: []` kosong, isi
    `type` yang hilang. Return (tools, n_fixed). Payload di-mutasi in-place
    (per-request, aman). Tak pernah melempar; bila gagal, tools dikembalikan
    apa adanya dengan n_fixed=0.
    """
    try:
        if not isinstance(tools, list):
            return tools, 0
        fixed = 0
        for tool in tools:
            if not isinstance(tool, dict):
                continue
            function = tool.get("function")
            holder = function if isinstance(function, dict) else tool
            for key in ("parameters", "inputSchema", "input_schema"):
                schema = holder.get(key)
                if not isinstance(schema, dict):
                    continue
                try:
                    defs: Dict[str, Any] = {}
                    _index_schema_defs(schema, (), defs, frozenset())
                    scc_of, cyclic_ids = _cyclic_scc_map(defs) if defs else ({}, set())
                    cleaned = _clean_schema_node(
                        schema, schema, (), (), frozenset(), 0, scc_of, cyclic_ids
                    )
                except (TypeError, ValueError, AttributeError, RecursionError):
                    continue
                if isinstance(cleaned, dict) and cleaned != schema:
                    holder[key] = cleaned
                    fixed += 1
        return tools, fixed
    except (AttributeError, TypeError, ValueError):
        try:
            return tools, 0
        except (TypeError, ValueError):
            return [], 0


# ── Klasifikasi 400 non-retryable (fail-fast, tanpa rotasi target) ──
# 400 payload-error (skema rekursif, schema invalid, tool tak dikenal,
# konteks kepanjangan, ...) TIDAK sembuh dengan ganti relay/proxy/direct:
# request identik gagal di semua egress. Tanpa fail-fast, 1 request buruk
# membakar 13 attempt x ~2 dtk + menandai proxy/relay sehat sebagai rusak.
# PENGECUALIAN: penolakan replay `encrypted_content` (400 juga) punya jalur
# auto-heal sendiri — helper ini sengaja return False untuknya.
_NON_RETRYABLE_400_MARKERS = (
    "recursive",
    "nesting",
    "too deep",
    "maximum depth",
    "invalid_request_error",
    "invalid schema",
    "additional properties",
    "unknown function",
    "unknown tool",
    "invalid tool",
    "invalid function",
    "unsupported",
    "not supported",
    "context length",
    "context_length",
    "token limit",
    "maximum context",
)


def _is_non_retryable_400(detail: Any) -> bool:
    """True bila detail 400 = salah payload (retry ke target lain sia-sia)."""
    try:
        if not detail or not isinstance(detail, str):
            return False
        lowered = detail.lower()
        if "encrypted_content" in lowered:
            return False
        return any(marker in lowered for marker in _NON_RETRYABLE_400_MARKERS)
    except (TypeError, ValueError, AttributeError):
        return False


def _dump_fatal_400(model: Any, payload: Any, status: Any, raw_body: Any) -> str:
    """Simpan wire `tools` + body upstream lengkap ke file bounded (overwrite).

    Dipakai saat FATAL-400: pesan provider di log terpotong (echo skema bisa
    puluhan KB) sehingga aturan penolakan persisnya tak terlihat. File
    `<tmp>/sinug-fatal-400.json` ditimpa tiap kejadian (bounded, bukan append)
    berisi wire tools persis yang dikirim + body upstream utuh (cap 1MB
    per sisi). Return path atau ''. Tak pernah melempar.
    """
    try:
        import json as _json
        import os as _os
        import tempfile as _tf
        import time as _time
        tools = payload.get("tools") if isinstance(payload, dict) else None
        try:
            wire_str = _json.dumps(tools, ensure_ascii=False)
        except (TypeError, ValueError):
            wire_str = "<unserializable>"
        try:
            if isinstance(raw_body, (bytes, bytearray)):
                up_str = bytes(raw_body).decode("utf-8", "replace")
            else:
                up_str = str(raw_body)
        except (TypeError, ValueError, AttributeError):
            up_str = "<undecodable>"
        try:
            wire_obj: Any = _json.loads(wire_str)
        except (TypeError, ValueError):
            wire_obj = wire_str[:1000000]
        if isinstance(wire_str, str) and len(wire_str) > 1000000:
            try:
                wire_obj = _json.loads(wire_str[:1000000])
            except (TypeError, ValueError):
                wire_obj = wire_str[:1000000]
        doc = {
            "ts": _time.strftime("%Y-%m-%dT%H:%M:%S"),
            "model": model if isinstance(model, str) else str(model),
            "upstream_status": status,
            "wire_tools": wire_obj,
            "upstream_body": up_str[:1000000],
        }
        path = _os.path.join(_tf.gettempdir(), "sinug-fatal-400.json")
        with open(path, "w", encoding="utf-8") as fh:
            _json.dump(doc, fh, ensure_ascii=False)
        return path
    except (OSError, TypeError, ValueError):
        return ""

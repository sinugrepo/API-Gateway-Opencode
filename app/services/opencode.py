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
import os
import secrets
import struct
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

from app.core.config import OPENCODE_CLIENT_NAME, OPENCODE_PROJECT, OPENCODE_SESSION_ID


_BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

# User-Agent persis CLI opencode asli (dari capture request nyata + doc §5).
# Bisa dioverride via env bila CLI upstream diupdate.
OPENCODE_USER_AGENT = os.getenv(
    "OPENCODE_USER_AGENT",
    "opencode/1.18.35 ai-sdk/provider-utils/4.0.40 runtime/bun/1.3.14",
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
    request_id: Optional[str] = None,
) -> Dict[str, str]:
    """Bangun header identitas CLI + User-Agent ala CLI asli.

    User-Agent disamakan dengan CLI opencode asli (`opencode-session.md` §5)
    agar fingerprint caller upstream konsisten — UA
    python-httpx bawaan justru menandai request ini BUKAN CLI (rawan
    ditolak free tier dengan FreeTierError).
    """
    return {
        "x-opencode-client": OPENCODE_CLIENT_NAME,
        "x-opencode-project": OPENCODE_PROJECT,
        "x-opencode-session": session_id or OPENCODE_SESSION_ID or _new_opencode_session_id(),
        "x-opencode-request": request_id or _new_opencode_request_id(),
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

    Header yang sudah dikirim klien (Hermes baru dsb.) dipakai apa adanya;
    yang hilang diisi ala CLI. Hasilnya dipakai untuk SEMUA upstream attempt
    dalam request ini agar sesi stabil lintas retry relay/direct.
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
    req_id = _get("x-opencode-request")
    headers = _opencode_cli_headers(
        session_id=session or None,
        request_id=req_id or None,
    )
    client = _get("x-opencode-client")
    if client:
        headers["x-opencode-client"] = client
    project = _get("x-opencode-project")
    if project:
        headers["x-opencode-project"] = project
    return headers


def _oc_session_tag(headers: Optional[Dict[str, str]]) -> str:
    """Penanda sesi singkat untuk log (10 char pertama, aman dibagikan)."""
    try:
        session = (headers or {}).get("x-opencode-session", "")
    except (AttributeError, TypeError):
        session = ""
    return f"ses={session[:14]}" if session else "ses=-"


def _fresh_request_headers(base: Optional[Dict[str, str]]) -> Dict[str, str]:
    """Salinan headers dengan `x-opencode-request` baru yang unik.

    Dipakai per upstream attempt (tiap rotasi relay + fallback direct):
    CLI asli mengirim `msg_...` unik per POST, dan memakai ulang satu
    request ID di semua attempt terlihat seperti replay di sisi upstream.
    Sesi (`x-opencode-session`) sengaja TIDAK diubah agar affinity cache
    + issuance encrypted_content tetap stabil dalam satu request klien.
    """
    headers = dict(base or {})
    headers["x-opencode-request"] = _new_opencode_request_id()
    return headers


def _fresh_identity_headers(base: Optional[Dict[str, str]]) -> Dict[str, str]:
    """Salinan headers dengan session DAN request ID yang sepenuhnya baru.

    HANYA untuk upaya terakhir fresh-session (semua target 403 termasuk
    direct): bila identitas lama yang di-flag upstream, identitas baru
    memberi peluang lolos. Format tetap valid ala CLI
    (`ses_`/`msg_` + 12 hex + 14 base62); header lain (client, project,
    User-Agent) dipertahankan agar fingerprint caller tetap konsisten.
    JANGAN dipakai untuk retry biasa — sesi stabil per-percakapan adalah
    syarat cache affinity + issuance encrypted_content (lihat
    `_stable_opencode_session`).
    """
    headers = dict(base or {})
    headers["x-opencode-session"] = _new_opencode_session_id()
    headers["x-opencode-request"] = _new_opencode_request_id()
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
        fresh_headers["x-opencode-request"] = _new_opencode_request_id()
        if proxy_url:
            new_targets.append((url, fresh_headers, proxy_url))
        else:
            new_targets.append((url, fresh_headers))
    if isinstance(payload, dict) and isinstance(payload.get("prompt_cache_key"), str):
        payload["prompt_cache_key"] = fresh_key
    return new_targets, fresh_session, fresh_key


# ── Free-tier client fingerprint gates ( diverifikasi live 2026-09-18 ) ──
# Upstream Zen menolak request free-tier dengan 403 FreeTierError bila salah
# satu gate tidak terpenuhi (lihat `fix opencode.ts`):
#   1. stream:true (stream:false / hilang -> 403, chat MAUPUN responses).
#   2. Kuartet tools bawaan OpenCode [bash, glob, grep, read] hadir di body.
# Bentuk tools berbeda per API: chat memakai bungkus
# {"type":"function","function":{...}}, responses memakai flat
# {"type":"function","name":...}. Klien (Hermes dsb.) jarang mengirim tools
# ini, jadi proxy menyuntikkannya agar terlihat seperti OpenCode CLI.
OPENCODE_FINGERPRINT_TOOLS = ("bash", "glob", "grep", "read")

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


def normalize_responses_tools(tools: Any) -> Optional[List[Dict[str, Any]]]:
    """Normalisasi tools chat/MCP menjadi bentuk flat Responses API.

    Kebalikan `normalize_chat_tools`: `{"type":"function","function":{...}}`
    (chat) maupun MCP `{"name":..., "inputSchema":...}` -> flat
    `{"type":"function","name":...,"parameters":...}`. `strict` dipertahankan.
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
                "name": name.strip(),
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
            "name": name.strip(),
            "description": tool.get("description") or f"Tool {name.strip()}",
            "parameters": schema if schema is not None else {"type": "object", "properties": {}},
        }
        if isinstance(tool.get("strict"), bool):
            entry["strict"] = tool["strict"]
        out.append(entry)
    return out or None


def ensure_chat_fingerprint_tools(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Suntik kuartet tools fingerprint bentuk chat bila belum ada."""
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
    for name in OPENCODE_FINGERPRINT_TOOLS:
        if name in present:
            continue
        tools.append({
            "type": "function",
            "function": {
                "name": name,
                "description": f"OpenCode built-in {name} tool",
                "parameters": {"type": "object", "properties": {}},
            },
        })
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
    """Suntik kuartet tools fingerprint bentuk responses bila belum ada."""
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
    for name in OPENCODE_FINGERPRINT_TOOLS:
        if name in present:
            continue
        tools.append({
            "type": "function",
            "name": name,
            "description": f"OpenCode built-in {name} tool",
            "parameters": {"type": "object", "properties": {}},
        })
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
    # Null eksplisit (temperature:null dsb. dari klien MCP) bila diteruskan
    # mentah berisiko 400 upstream — drop agar upstream pakai default-nya.
    for _opt in ("temperature", "top_p"):
        if _opt in payload and payload.get(_opt) is None:
            payload.pop(_opt, None)
    payload["store"] = False
    ensure_responses_fingerprint_tools(payload)
    ensure_spark_reasoning_xhigh(payload)
    coerce_tool_choice_auto(payload, "responses-wire")
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

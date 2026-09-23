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
import time
from typing import Any, Dict, List, Optional

from app.core.config import OPENCODE_CLIENT_NAME, OPENCODE_PROJECT, OPENCODE_SESSION_ID


_BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

# User-Agent persis CLI opencode asli (dari capture request nyata + doc §5).
# Bisa dioverride via env bila CLI upstream diupdate.
OPENCODE_USER_AGENT = os.getenv(
    "OPENCODE_USER_AGENT",
    "opencode/1.18.31 ai-sdk/provider-utils/4.0.40 runtime/bun/1.3.14",
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

    User-Agent disamakan dengan CLI opencode asli (`opencode-session.md` §5:
    `opencode/1.18.31 ...`) agar fingerprint caller upstream konsisten — UA
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


_STABLE_SESSION_FALLBACK = "ses_" + "0" * 12 + "0" * 14  # format-valid bila tak ada sinyal


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
      `ses_` + 12 hex + 14 base62.
    Deterministik dari fingerprint sehingga stabil antar-turn DAN antar
    restart (cache in-memory hilang saat restart, tapi hash sama -> ID sama).

    Prioritas:
    1. OPENCODE_SESSION_ID env (statis global, opsional).
    2. Fingerprint percakapan (prompt_cache_key / system+first user msg) ->
       sesi sama otomatis untuk percakapan sama, beda untuk percakapan beda.
    3. Fallback konstan bila payload tak punya sinyal sama sekali.
    """
    if OPENCODE_SESSION_ID:
        return OPENCODE_SESSION_ID
    fingerprint = _conversation_fingerprint(payload)
    if fingerprint:
        # 12 hex pertama fingerprint = prefix hex valid (lowercase sha256).
        # 14 base62 sisanya dipetakan deterministik dari sisa hash agar
        # suffix penuh 26 char valid + stabil (dua percakapan berbeda hampir
        # pasti beda: ruang 16^12 * 62^14).
        hex_prefix = fingerprint[:12].lower()
        remainder_int = int(fingerprint[12:], 16)
        chars: List[str] = []
        for _ in range(14):
            remainder_int, rem = divmod(remainder_int, 62)
            chars.append(_BASE62_ALPHABET[rem])
        return "ses_" + hex_prefix + "".join(chars)
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
    """Ambil nama tool dari bentuk chat maupun responses (toleran)."""
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


def ensure_chat_fingerprint_tools(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Suntik kuartet tools fingerprint bentuk chat bila belum ada."""
    if not isinstance(payload, dict):
        return payload
    tools = payload.get("tools")
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
    return payload


def ensure_responses_fingerprint_tools(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Suntik kuartet tools fingerprint bentuk responses bila belum ada."""
    if not isinstance(payload, dict):
        return payload
    tools = payload.get("tools")
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
    payload["store"] = False
    ensure_responses_fingerprint_tools(payload)
    ensure_spark_reasoning_xhigh(payload)
    return payload

"""OpenCode CLI identity headers (free-tier session)."""
import hashlib
import os
import secrets
import string
from typing import Any, Dict, List, Optional

from app.core.config import OPENCODE_CLIENT_NAME, OPENCODE_PROJECT, OPENCODE_SESSION_ID


_OPENCODE_ID_ALPHABET = string.ascii_letters + string.digits

# User-Agent persis CLI opencode asli (dari capture request nyata).
# Bisa dioverride via env bila CLI upstream diupdate.
OPENCODE_USER_AGENT = os.getenv(
    "OPENCODE_USER_AGENT",
    "opencode/1.18.29 ai-sdk/provider-utils/4.0.38 runtime/bun/1.3.14",
)


def _new_opencode_id(prefix: str, length: int) -> str:
    """ID opaque gaya CLI: prefix + alfanumerik acak (tidak ada info user)."""
    return prefix + "".join(secrets.choice(_OPENCODE_ID_ALPHABET) for _ in range(length))


def _new_opencode_session_id() -> str:
    return _new_opencode_id("ses_", 26)


def _new_opencode_request_id() -> str:
    return _new_opencode_id("msg_", 24)


def _opencode_cli_headers(
    *,
    session_id: Optional[str] = None,
    request_id: Optional[str] = None,
) -> Dict[str, str]:
    """Bangun header identitas CLI + User-Agent ala CLI asli.

    User-Agent disamakan dengan CLI opencode asli (referensi request nyata:
    `opencode/1.18.29 ai-sdk/provider-utils/4.0.38 runtime/bun/1.3.14`) agar
    fingerprint caller upstream konsisten — UA python-httpx bawaan justru
    menandai request ini BUKAN CLI (rawan ditolak free tier).
    """
    return {
        "x-opencode-client": OPENCODE_CLIENT_NAME,
        "x-opencode-project": OPENCODE_PROJECT,
        "x-opencode-session": session_id or OPENCODE_SESSION_ID or _new_opencode_session_id(),
        "x-opencode-request": request_id or _new_opencode_request_id(),
        "User-Agent": OPENCODE_USER_AGENT,
    }


_SESSION_ALPHABET = string.ascii_letters + string.digits  # ala CLI asli: ses_ + 26 alfanumerik

_STABLE_SESSION_FALLBACK = "ses_" + "0" * 26  # dipakai bila payload tak bisa di-fingerprint


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
    if isinstance(items, list):
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
        # Petakan hash deterministik ke karakter alfanumerik agar bentuknya
        # identik dengan sesi CLI asli (mis. ses_f663b1124ffer0M79j4R6q4Agi):
        # ses_ + 26 alfanumerik, huruf besar-kecil campur. Dua percakapan
        # berbeda hampir pasti menghasilkan sesi berbeda (36^26 ruang).
        digest_int = int(fingerprint, 16)
        chars = []
        for i in range(26):
            digest_int, rem = divmod(digest_int, 62)
            chars.append(_SESSION_ALPHABET[rem])
        return "ses_" + "".join(chars)
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

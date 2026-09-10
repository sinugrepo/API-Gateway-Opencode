"""OpenCode CLI identity headers (free-tier session)."""
import secrets
import string
from typing import Any, Dict, Optional

from app.core.config import OPENCODE_CLIENT_NAME, OPENCODE_PROJECT, OPENCODE_SESSION_ID


_OPENCODE_ID_ALPHABET = string.ascii_letters + string.digits


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
    """Bangun 4 header identitas CLI. Nilai eksplisit diutamakan."""
    return {
        "x-opencode-client": OPENCODE_CLIENT_NAME,
        "x-opencode-project": OPENCODE_PROJECT,
        "x-opencode-session": session_id or OPENCODE_SESSION_ID or _new_opencode_session_id(),
        "x-opencode-request": request_id or _new_opencode_request_id(),
    }


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

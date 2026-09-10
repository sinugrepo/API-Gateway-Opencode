"""Colored logging + in-memory live-log ring buffer."""
import threading
from collections import deque
from datetime import datetime
from typing import Dict, Tuple

from app.core.config import LIVE_LOG_MAXLEN


_LOG_COLORS: Dict[str, str] = {
    "INFO": "\033[92m",
    "WARN": "\033[93m",
    "ERROR": "\033[91m",
    "RELAY": "\033[96m",
    "STREAM": "\033[94m",
    "MODELS": "\033[95m",
    "USAGE": "\033[95m",
}


_RESET = "\033[0m"


_live_logs: deque = deque(maxlen=LIVE_LOG_MAXLEN)


_live_log_lock = threading.Lock()


_live_log_seq = 0


def _log(level: str, *args) -> None:
    color = _LOG_COLORS.get(level, "")
    timestamp = datetime.now().strftime("%H:%M:%S")
    try:
        text = " ".join(str(a) for a in args)
    except Exception:
        text = "<unformattable log args>"
    try:
        print(f"[{timestamp}] [{color}{level}{_RESET}]", *args, flush=False)
    except Exception:
        pass
    # Simpan ke ring buffer — tidak boleh melempar / memblokir request path.
    try:
        global _live_log_seq
        with _live_log_lock:
            _live_log_seq += 1
            # Dict kecil & flat agar JSON SSE murah diserialisasi.
            _live_logs.append(
                {
                    "id": _live_log_seq,
                    "ts": timestamp,
                    "level": str(level),
                    "msg": text[:2000],
                }
            )
    except Exception:
        pass


def _get_live_logs(
    since: int = 0, level: str = "", limit: int = 200
) -> Tuple[list, int]:
    """Ambil log dengan id > since, opsional filter level. O(N<=maxlen)."""
    try:
        limit = max(1, min(int(limit or 200), 500))
    except (TypeError, ValueError):
        limit = 200
    try:
        since = int(since or 0)
    except (TypeError, ValueError):
        since = 0
    wanted = (level or "").strip().upper()
    with _live_log_lock:
        latest = _live_log_seq
        if not _live_logs:
            return [], latest
        if wanted:
            out = [e for e in _live_logs if e["id"] > since and e["level"] == wanted]
        else:
            out = [e for e in _live_logs if e["id"] > since]
    if len(out) > limit:
        out = out[-limit:]
    return out, latest

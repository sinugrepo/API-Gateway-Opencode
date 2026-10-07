"""Runtime relay pool + egress settings (website-configurable, tanpa hardcoded).

Menggantikan daftar relay statis `_DEFAULT_RELAYS` / env `RELAY_URLS` yang
sebelumnya hanya bisa diubah via restart. Pola sama seperti outbound_proxy:

- Seed awal dari env (`RELAY_URLS` / `RELAY_URL`) + defaults bawaan.
- File `relays.json` (atomik tmp+rename) menyimpan URL + enabled + saklar
  egress (`use_relay`, `relay_fallback`, `egress_order`).
- `None` pada saklar = "ikut env" (backward-compat 100%).
- Tidak pernah melempar ke request path (semua helper defensif).
"""

import hashlib
import json
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from app.core.config import (
    RELAY_FALLBACK as _ENV_FALLBACK,
    RELAY_URLS as _ENV_RELAYS,
    USE_PROXY as _ENV_USE_PROXY,
    USE_RELAY as _ENV_USE_RELAY,
)
from app.core.logging_utils import _log

EGRESS_ORDERS = ("relay_first", "proxy_first")

_store_lock = threading.Lock()
_relays: Dict[str, Dict[str, Any]] = {}
_relays_order: List[str] = []
_use_relay_override: Optional[bool] = None
_fallback_override: Optional[bool] = None
_use_proxy_override: Optional[bool] = None
_egress_order: str = "relay_first"
_loaded = False


def _relay_id_for(url: str) -> str:
    try:
        norm = (url or "").strip().lower()
        return hashlib.sha256(norm.encode()).hexdigest()[:12]
    except (TypeError, ValueError, AttributeError):
        return hashlib.sha256(str(time.time()).encode()).hexdigest()[:12]


def _config_path() -> Path:
    try:
        import os as _os

        custom = (_os.getenv("RELAY_CONFIG_PATH") or "").strip()
        if custom:
            return Path(custom)
    except (TypeError, ValueError, AttributeError):
        pass
    try:
        return Path(__file__).resolve().parents[2] / "relays.json"
    except (IndexError, OSError, ValueError):
        return Path("./relays.json")


def _normalize(url: str) -> str:
    try:
        from app.core.config import _normalize_relay_url as _n

        return _n(url or "")
    except (ImportError, AttributeError, TypeError, ValueError):
        pass
    try:
        cleaned = (url or "").strip().rstrip("\\/").strip()
        if not cleaned:
            return ""
        if "://" not in cleaned:
            cleaned = f"https://{cleaned}"
        parsed = urlparse(cleaned)
        if not parsed.hostname:
            return ""
        path = parsed.path or ""
        if path in ("", "/"):
            cleaned = f"{parsed.scheme}://{parsed.netloc}/api/relay"
        return cleaned
    except (TypeError, ValueError, AttributeError):
        return ""


def _save_locked() -> None:
    path = _config_path()
    try:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except (OSError, ValueError):
            pass
        payload = {
            "use_relay": _use_relay_override,
            "relay_fallback": _fallback_override,
            "use_proxy": _use_proxy_override,
            "egress_order": _egress_order,
            "updated_at": int(time.time()),
            "relays": [
                {"url": e.get("url"), "enabled": bool(e.get("enabled", True))}
                for pid in _relays_order
                for e in [_relays.get(pid)]
                if isinstance(e, dict) and e.get("url")
            ],
        }
        tmp = path.with_suffix(path.suffix + ".tmp" if path.suffix else ".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        try:
            import os as _os

            _os.replace(str(tmp), str(path))
        except (OSError, ValueError):
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        try:
            import os as _os2

            _os2.chmod(str(path), 0o600)
        except (OSError, ValueError, AttributeError):
            pass
    except (OSError, ValueError, TypeError):
        _log("WARN", "relay-config: gagal menyimpan relays.json (non-fatal)")


def ensure_loaded() -> None:
    global _loaded, _use_relay_override, _fallback_override, _use_proxy_override, _egress_order
    with _store_lock:
        if _loaded:
            return
        _loaded = True
        # 1. Seed dari env/defaults (source=env).
        try:
            for raw in list(_ENV_RELAYS or []):
                norm = _normalize(raw)
                if not norm:
                    continue
                pid = _relay_id_for(norm)
                if pid not in _relays:
                    _relays[pid] = {
                        "id": pid,
                        "url": norm,
                        "enabled": True,
                        "source": "env",
                    }
                    _relays_order.append(pid)
        except (TypeError, ValueError, AttributeError):
            pass
        # 2. File menimpa/melengkapi (source=file).
        path = _config_path()
        try:
            if not path.is_file():
                return
            data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
            if not isinstance(data, dict):
                if isinstance(data, list):
                    data = {"relays": data}
                else:
                    return
            # Saklar egress (None = ikut env).
            if data.get("use_relay") is None:
                _use_relay_override = None
            else:
                _use_relay_override = bool(data.get("use_relay"))
            if data.get("relay_fallback") is None:
                _fallback_override = None
            else:
                _fallback_override = bool(data.get("relay_fallback"))
            if data.get("use_proxy") is None:
                _use_proxy_override = None
            else:
                _use_proxy_override = bool(data.get("use_proxy"))
            order = str(data.get("egress_order") or "relay_first").strip().lower()
            _egress_order = order if order in EGRESS_ORDERS else "relay_first"
            entries = data.get("relays", [])
            if not isinstance(entries, list):
                return
            # File adalah authoritative untuk DAFTAR: reset lalu isi ulang
            # agar relay yang dihapus via website tidak muncul lagi dari seed.
            _relays.clear()
            _relays_order.clear()
            for item in entries:
                try:
                    raw_url = item.get("url") if isinstance(item, dict) else item
                    norm = _normalize(str(raw_url or ""))
                    if not norm:
                        continue
                    enabled = bool(item.get("enabled", True)) if isinstance(item, dict) else True
                    pid = _relay_id_for(norm)
                    _relays[pid] = {
                        "id": pid,
                        "url": norm,
                        "enabled": enabled,
                        "source": "file",
                    }
                    if pid not in _relays_order:
                        _relays_order.append(pid)
                except (TypeError, ValueError, AttributeError):
                    continue
            # File kosong eksplisit = operator mematikan semua relay
            # (jangan seed ulang dari env — hormati pilihan operator).
        except (OSError, ValueError, TypeError, AttributeError):
            pass


# ---- Effective getters (dipakai request path, tidak pernah melempar) ----

def get_effective_use_relay() -> bool:
    try:
        ensure_loaded()
        with _store_lock:
            if _use_relay_override is not None:
                return bool(_use_relay_override)
        return bool(_ENV_USE_RELAY)
    except (TypeError, ValueError, AttributeError):
        return True


def get_effective_fallback() -> bool:
    try:
        ensure_loaded()
        with _store_lock:
            if _fallback_override is not None:
                return bool(_fallback_override)
        return bool(_ENV_FALLBACK)
    except (TypeError, ValueError, AttributeError):
        return True


def get_effective_use_proxy() -> bool:
    """Saklar proxy efektif: override website > env. File global dicek terpisah."""
    try:
        ensure_loaded()
        with _store_lock:
            if _use_proxy_override is not None:
                return bool(_use_proxy_override)
        return bool(_ENV_USE_PROXY)
    except (TypeError, ValueError, AttributeError):
        return True


def get_egress_order() -> str:
    try:
        ensure_loaded()
        with _store_lock:
            return _egress_order if _egress_order in EGRESS_ORDERS else "relay_first"
    except (TypeError, ValueError, AttributeError):
        return "relay_first"


def get_effective_relays() -> List[str]:
    """URL relay ENABLED sesuai urutan (untuk rotasi request path)."""
    try:
        ensure_loaded()
        with _store_lock:
            return [
                str(_relays[pid].get("url"))
                for pid in list(_relays_order)
                if isinstance(_relays.get(pid), dict)
                and _relays[pid].get("enabled")
                and _relays[pid].get("url")
            ]
    except (TypeError, ValueError, AttributeError):
        return []


def get_all_relays() -> List[str]:
    return get_effective_relays()


# ---- Dashboard CRUD ----

def _penalty_snapshot() -> Tuple[List[str], List[str]]:
    try:
        from app.services.relay import _relay_penalty as _pen, _relay_stream_broken as _brk

        now = time.time()
        pen = [k for k, v in list(_pen.items()) if v > now]
        brk = [k for k, v in list(_brk.items()) if v > now]
        return pen, brk
    except (ImportError, AttributeError, TypeError, ValueError):
        return [], []


def list_relays() -> List[Dict[str, Any]]:
    ensure_loaded()
    pen, brk = _penalty_snapshot()
    pen_set = set(pen)
    brk_set = set(brk)
    out: List[Dict[str, Any]] = []
    with _store_lock:
        for pid in list(_relays_order):
            e = _relays.get(pid)
            if not isinstance(e, dict):
                continue
            url = str(e.get("url") or "")
            try:
                host = url.split("//", 1)[-1].split("/", 1)[0]
            except (TypeError, AttributeError, IndexError):
                host = url
            out.append(
                {
                    "id": pid,
                    "url": url,
                    "host": host,
                    "enabled": bool(e.get("enabled", True)),
                    "source": e.get("source", "file"),
                    "penalized": url in pen_set,
                    "stream_broken": url in brk_set,
                }
            )
    return out


def get_relay_overview() -> Dict[str, Any]:
    try:
        ensure_loaded()
        relays = list_relays()
        enabled = [r for r in relays if r.get("enabled")]
        with _store_lock:
            use_ov = _use_relay_override
            fb_ov = _fallback_override
            px_ov = _use_proxy_override
            order = _egress_order
        pen, brk = _penalty_snapshot()
        return {
            "use_relay": get_effective_use_relay(),
            "use_relay_override": use_ov,
            "use_relay_env": bool(_ENV_USE_RELAY),
            "relay_fallback": get_effective_fallback(),
            "fallback_override": fb_ov,
            "fallback_env": bool(_ENV_FALLBACK),
            "use_proxy": get_effective_use_proxy(),
            "use_proxy_override": px_ov,
            "use_proxy_env": bool(_ENV_USE_PROXY),
            "egress_order": order,
            "count": len(relays),
            "enabled_count": len(enabled),
            "penalized_count": len(pen),
            "stream_broken_count": len(brk),
            "relays": relays,
        }
    except (TypeError, ValueError, AttributeError):
        return {
            "use_relay": True, "relay_fallback": True, "egress_order": "relay_first",
            "count": 0, "enabled_count": 0, "relays": [],
        }


def add_relay(raw_url: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        ensure_loaded()
        norm = _normalize(raw_url or "")
        if not norm:
            return None, "URL relay tidak valid (contoh: https://relay-x.vercel.app/api/relay)"
        parsed = urlparse(norm)
        if not parsed.hostname:
            return None, "URL relay tidak valid"
        pid = _relay_id_for(norm)
        with _store_lock:
            if pid in _relays:
                return None, "relay sudah ada (duplikat URL)"
            if len(_relays_order) >= 64:
                return None, "batas 64 relay tercapai"
            _relays[pid] = {"id": pid, "url": norm, "enabled": True, "source": "file"}
            _relays_order.append(pid)
            _save_locked()
            entry = dict(_relays[pid])
        _log("RELAY", f"relay ditambah {norm}")
        return entry, None
    except (TypeError, ValueError, AttributeError) as exc:
        return None, f"input tidak valid: {exc}"


def remove_relay(pid: str) -> bool:
    try:
        ensure_loaded()
        pid = (pid or "").strip()
        if not pid:
            return False
        with _store_lock:
            if pid not in _relays:
                return False
            _relays.pop(pid, None)
            try:
                _relays_order.remove(pid)
            except ValueError:
                pass
            _save_locked()
            return True
    except (TypeError, ValueError, AttributeError):
        return False


def set_relay_enabled(pid: str, enabled: bool) -> bool:
    try:
        ensure_loaded()
        pid = (pid or "").strip()
        with _store_lock:
            e = _relays.get(pid)
            if not isinstance(e, dict):
                return False
            e["enabled"] = bool(enabled)
            _save_locked()
            return True
    except (TypeError, ValueError, AttributeError):
        return False


def set_egress_settings(
    use_relay: Any = "__unset__",
    relay_fallback: Any = "__unset__",
    egress_order: Any = "__unset__",
    use_proxy: Any = "__unset__",
) -> Dict[str, Any]:
    """Simpan saklar egress. `None` = kembali ikut env. Return overview."""
    global _use_relay_override, _fallback_override, _egress_order, _use_proxy_override
    ensure_loaded()

    def _to_opt_bool(v: Any) -> Any:
        if v == "__unset__":
            return "__unset__"
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("null", "none", "auto", "env", ""):
                return None
            return s in ("1", "true", "on", "yes")
        return bool(v)

    with _store_lock:
        if use_relay != "__unset__":
            _use_relay_override = _to_opt_bool(use_relay)
        if relay_fallback != "__unset__":
            _fallback_override = _to_opt_bool(relay_fallback)
        if use_proxy != "__unset__":
            _use_proxy_override = _to_opt_bool(use_proxy)
        if egress_order != "__unset__" and egress_order is not None:
            o = str(egress_order or "").strip().lower()
            if o in EGRESS_ORDERS:
                _egress_order = o
        _save_locked()
    _log(
        "RELAY",
        f"egress config via monitor: use_relay={_use_relay_override} "
        f"fallback={_fallback_override} use_proxy={_use_proxy_override} "
        f"order={_egress_order}",
    )
    return get_relay_overview()


def reorder_targets_proxy_first(
    targets: List[Any],
) -> List[Any]:
    """Susun ulang kandidat agar proxy-direct di depan relay (proxy_first).

    `targets` = list 2/3-tuple hasil expand (relay + proxy + direct).
    relay_first = tanpa perubahan. proxy_first = [proxy..., relay..., direct].
    Idempoten & tidak pernah melempar.
    """
    try:
        if get_egress_order() != "proxy_first":
            return targets
        relays, proxies, directs = [], [], []
        try:
            from app.services.outbound_proxy import _as_triple
        except (ImportError, AttributeError):
            _as_triple = None  # type: ignore
        for t in targets or []:
            try:
                if _as_triple is not None:
                    conv = _as_triple(t)
                    if conv is None:
                        directs.append(t)
                        continue
                    _, headers, proxy = conv
                else:
                    headers = t[1] if len(t) > 1 else {}
                    proxy = t[2] if len(t) > 2 else None
                is_relay = "x-relay-target" in (headers or {})
                if is_relay:
                    relays.append(t)
                elif proxy:
                    proxies.append(t)
                else:
                    directs.append(t)
            except (TypeError, ValueError, AttributeError, IndexError):
                directs.append(t)
        if proxies:
            _log("RELAY", f"proxy-first: {len(proxies)} proxy-direct di depan {len(relays)} relay")
        return proxies + relays + directs
    except (TypeError, ValueError, AttributeError):
        return targets

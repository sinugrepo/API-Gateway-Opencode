"""Outbound proxy pool (SOCKS5 / HTTP) untuk request DIRECT ke upstream.

Lapisan egress TAMBAHAN selain relay Vercel. Setiap proxy = satu egress IP
yang berbeda ke opencode.ai (mis. 10x warp-socks lokal di 127.0.0.1:40001+).

Desain anti-bug:
- Kosong = no-op total (semua helper mengembalikan list kosong / None).
- Tidak pernah melempar ke pemanggil request path (semua error ditangkap,
  fallback ke direct mentah selalu tersedia).
- Password TIDAK PERNAH dikembalikan ke dashboard / log (hanya mask `***`).
- Round-robin PER REQUEST (seperti relay) + cooldown per-proxy saat gagal.
- Persistensi file JSON atomik (tmp + rename), tidak pernah crash start
  walau file rusak (diabaikan + log).
- Thread-safe via lock (index, penalty, store).
"""
import hashlib
import json
import re
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx

from app.core.config import (
    OUTBOUND_PROXIES_ENV,
    PROXY_403_COOLDOWN,
    PROXY_CONFIG_PATH,
    PROXY_COOLDOWN,
    PROXY_MAX_COUNT,
    PROXY_TEST_TIMEOUT,
    REQUEST_TIMEOUT,
    USE_PROXY,
)
from app.core.logging_utils import _log

_ALLOWED_SCHEMES = ("socks5", "socks5h", "http", "https")
# Alias umum -> kanonik. `socks` tanpa versi = socks5h (DNS via proxy agar
# tidak bocor + cocok untuk warp). httpx/httpcore menerima socks5 & socks5h.
_SCHEME_ALIASES = {"socks": "socks5h", "socks5": "socks5", "socks5h": "socks5h"}

_HOST_RE = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9.\-]{0,253}[A-Za-z0-9])?$")

_store_lock = threading.Lock()
_proxy_index = 0
_proxy_index_lock = threading.Lock()
_proxy_penalty: Dict[str, float] = {}
_proxy_penalty_lock = threading.Lock()

# 403 FreeTierError via proxy = egress IP di-flag upstream (BUKAN proxy rusak:
# koneksi SOCKS/HTTP-nya sehat, test-ipify OK). Dilacak TERPISAH dari transport
# agar dashboard bisa menampilkan DOWN vs FLAGGED dan durasinya bisa
# dibedakan (PROXY_403_COOLDOWN vs PROXY_COOLDOWN).
_proxy_flagged: Dict[str, float] = {}
_proxy_flagged_lock = threading.Lock()

# Toleransi blip transport: kegagalan transport tunggal (mis. warp warp-socks
# yang flaky — gagal sekali lalu sehat lagi) hanya disisihkan SEBENTAR, bukan
# langsung full cooldown. Hanya kegagalan transport BERUNTUN dalam jendela
# waktu yang memakai cooldown penuh. Format: pid -> (count, last_ts).
_proxy_flap: Dict[str, Any] = {}
_proxy_flap_lock = threading.Lock()
_FLAP_WINDOW_S = 120.0
_FLAP_FIRST_COOLDOWN_S = 15.0

# In-memory store: id -> entry dict
# entry = {id, scheme, host, port, username, password, enabled, source}
_proxies: Dict[str, Dict[str, Any]] = {}
_proxies_order: List[str] = []
_global_enabled = True  # di-override file/env saat load
_loaded = False


def _normalize_proxy_scheme(raw: str) -> str:
    s = (raw or "").strip().lower()
    if s in _SCHEME_ALIASES:
        return _SCHEME_ALIASES[s]
    if s in _ALLOWED_SCHEMES:
        return s
    return ""


def normalize_proxy_url(raw: str) -> str:
    """Normalisasi URL proxy -> `scheme://[user:pass@]host:port` atau "".

    - Tanpa skema -> `socks5h://` (kasus utama: `127.0.0.1:40001`).
    - `socks://` -> `socks5h://`.
    - Path/query/fragment dibuang (proxy URL hanya host:port).
    - Host wajib, port wajib 1-65535 (default per-skema bila hilang:
      http/https=8080? TIDAK — port wajib eksplisit agar typo ketahuan).
    - Return "" bila tidak valid (tidak pernah melempar).
    """
    try:
        cleaned = (raw or "").strip()
        if not cleaned:
            return ""
        # Bersihkan typo paste umum.
        cleaned = cleaned.strip().rstrip("\\/").strip()
        if not cleaned:
            return ""
        if "://" not in cleaned:
            cleaned = f"socks5h://{cleaned}"
        parsed = urlparse(cleaned)
        scheme = _normalize_proxy_scheme(parsed.scheme or "")
        if not scheme:
            return ""
        host = (parsed.hostname or "").strip().lower()
        if not host or len(host) > 253:
            return ""
        # Tolak userinfo aneh / karakter berbahaya di host.
        if not _HOST_RE.match(host) and not _is_ip_literal(host):
            return ""
        port = parsed.port
        if not isinstance(port, int) or not (1 <= port <= 65535):
            return ""
        username = parsed.username or ""
        password = parsed.password or ""
        # Batas panjang kredensial (anti-abuse).
        if len(username) > 128 or len(password) > 256:
            return ""
        auth = ""
        if username:
            # Quote minimal: httpx menerima user:pass mentah di URL.
            auth = username
            if password:
                auth += f":{password}"
            auth += "@"
        return f"{scheme}://{auth}{host}:{port}"
    except (ValueError, TypeError, AttributeError):
        return ""


def _is_ip_literal(host: str) -> bool:
    try:
        import ipaddress
        ipaddress.ip_address(host)
        return True
    except (ValueError, TypeError):
        return False


def proxy_connection_url(entry: Dict[str, Any]) -> str:
    """Bangun URL koneksi httpx dari entry store (termasuk kredensial)."""
    try:
        scheme = entry.get("scheme", "socks5h")
        host = entry.get("host", "")
        port = int(entry.get("port", 0))
        user = entry.get("username") or ""
        pwd = entry.get("password") or ""
        auth = f"{user}:{pwd}@" if user and pwd else (f"{user}@" if user else "")
        return f"{scheme}://{auth}{host}:{port}"
    except (TypeError, ValueError, AttributeError):
        return ""


def display_proxy_url(entry_or_url: Any) -> str:
    """Versi aman-log: password diganti `***`. Tidak pernah melempar."""
    try:
        if isinstance(entry_or_url, dict):
            scheme = entry_or_url.get("scheme", "?")
            host = entry_or_url.get("host", "?")
            port = entry_or_url.get("port", "?")
            user = entry_or_url.get("username") or ""
            prefix = f"{user}:***@" if user and entry_or_url.get("password") else (f"{user}@" if user else "")
            return f"{scheme}://{prefix}{host}:{port}"
        url = str(entry_or_url or "")
        parsed = urlparse(url)
        if not parsed.hostname:
            return "(invalid-proxy)"
        if parsed.username and parsed.password:
            return f"{parsed.scheme}://{parsed.username}:***@{parsed.hostname}:{parsed.port}"
        if parsed.username:
            return f"{parsed.scheme}://{parsed.username}@{parsed.hostname}:{parsed.port}"
        return f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
    except (TypeError, ValueError, AttributeError):
        return "(invalid-proxy)"


def _proxy_id_for(scheme: str, host: str, port: int, username: str = "") -> str:
    """ID stabil 12-hex dari identitas proxy (tanpa password!)."""
    try:
        base = f"{scheme.lower()}://{username.lower()}@{host.lower()}:{int(port)}"
        return hashlib.sha256(base.encode()).hexdigest()[:12]
    except (TypeError, ValueError, AttributeError):
        return hashlib.sha256(str(time.time()).encode()).hexdigest()[:12]


def _config_path() -> Path:
    try:
        return Path(PROXY_CONFIG_PATH)
    except (TypeError, ValueError):
        return Path("./proxies.json")


def _load_file_entries() -> Tuple[List[Dict[str, Any]], bool]:
    """Baca file JSON -> (entries, global_enabled). Rusak = ([], True)."""
    path = _config_path()
    try:
        if not path.is_file():
            return [], True
        text = path.read_text(encoding="utf-8", errors="replace")
        data = json.loads(text)
        if isinstance(data, list):
            # Format lama: langsung list.
            return data if isinstance(data, list) else [], True
        if isinstance(data, dict):
            entries = data.get("proxies", [])
            enabled = data.get("enabled", True)
            if not isinstance(entries, list):
                entries = []
            return entries, bool(enabled)
        return [], True
    except (OSError, ValueError, TypeError, AttributeError):
        return [], True


def _save_file_locked() -> None:
    """Tulis store ke file secara atomik. Dipanggil di bawah _store_lock."""
    path = _config_path()
    try:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except (OSError, ValueError):
            pass
        payload = {
            "enabled": _global_enabled,
            "updated_at": int(time.time()),
            "proxies": [
                {
                    "scheme": e.get("scheme"),
                    "host": e.get("host"),
                    "port": e.get("port"),
                    "username": e.get("username") or "",
                    # Password runtime (file 0600 bila bisa). Ini satu-satunya
                    # tempat password disimpan di disk.
                    "password": e.get("password") or "",
                    "enabled": bool(e.get("enabled", True)),
                }
                for pid in _proxies_order
                for e in [_proxies.get(pid)]
                if isinstance(e, dict) and e.get("source") != "env"
            ],
        }
        tmp = path.with_suffix(path.suffix + ".tmp" if path.suffix else ".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        try:
            import os as _os
            _os.replace(str(tmp), str(path))
        except (OSError, ValueError):
            # Fallback non-atomik bila replace gagal (mis. Windows lock).
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        try:
            import os as _os2
            _os2.chmod(str(path), 0o600)
        except (OSError, ValueError, AttributeError):
            pass
    except (OSError, ValueError, TypeError):
        _log("WARN", "proxy-config: gagal menyimpan proxies.json (non-fatal)")


def _upsert_entry(entry: Dict[str, Any]) -> None:
    pid = str(entry.get("id") or "")
    if not pid:
        return
    if pid not in _proxies:
        if len(_proxies_order) >= PROXY_MAX_COUNT:
            return
        _proxies_order.append(pid)
    _proxies[pid] = entry


def ensure_loaded() -> None:
    """Load sekali: env seed + file. Aman dipanggil berkali-kali."""
    global _loaded, _global_enabled
    with _store_lock:
        if _loaded:
            return
        _loaded = True
        # 1. Seed dari env (source=env, tidak disimpan ke file).
        try:
            for raw in (OUTBOUND_PROXIES_ENV or "").split(","):
                norm = normalize_proxy_url(raw)
                if not norm:
                    continue
                try:
                    p = urlparse(norm)
                    pid = _proxy_id_for(p.scheme, p.hostname or "", p.port or 0, p.username or "")
                    _proxies[pid] = {
                        "id": pid,
                        "scheme": p.scheme,
                        "host": (p.hostname or "").lower(),
                        "port": int(p.port or 0),
                        "username": p.username or "",
                        "password": p.password or "",
                        "enabled": True,
                        "source": "env",
                    }
                    if pid not in _proxies_order:
                        _proxies_order.append(pid)
                except (ValueError, TypeError, AttributeError):
                    continue
        except (TypeError, ValueError, AttributeError):
            pass
        # 2. File (source=file/runtime, menimpa seed bila id sama).
        file_entries, file_enabled = _load_file_entries()
        _global_enabled = bool(file_enabled)
        for item in file_entries:
            try:
                if not isinstance(item, dict):
                    continue
                if isinstance(item.get("url"), str):
                    norm = normalize_proxy_url(item.get("url") or "")
                    if not norm:
                        continue
                    p = urlparse(norm)
                    scheme, host, port = p.scheme, (p.hostname or "").lower(), int(p.port or 0)
                    user, pwd = p.username or "", p.password or ""
                    enabled = bool(item.get("enabled", True))
                else:
                    scheme = _normalize_proxy_scheme(str(item.get("scheme") or ""))
                    host = str(item.get("host") or "").strip().lower()
                    try:
                        port = int(item.get("port") or 0)
                    except (TypeError, ValueError):
                        continue
                    if not scheme or not host or not (1 <= port <= 65535):
                        continue
                    user = str(item.get("username") or "")[:128]
                    pwd = str(item.get("password") or "")[:256]
                    enabled = bool(item.get("enabled", True))
                pid = _proxy_id_for(scheme, host, port, user)
                _proxies[pid] = {
                    "id": pid,
                    "scheme": scheme,
                    "host": host,
                    "port": port,
                    "username": user,
                    "password": pwd,
                    "enabled": enabled,
                    "source": "file",
                }
                if pid not in _proxies_order:
                    if len(_proxies_order) >= PROXY_MAX_COUNT:
                        break
                    _proxies_order.append(pid)
            except (TypeError, ValueError, AttributeError):
                continue


def _effective_use_proxy_env() -> bool:
    """USE_PROXY efektif: override website (relays.json) > env.

    Tidak pernah melempar. Dipakai semua gerbang lapisan proxy agar saklar
    di website benar-benar mematikan/menyalakan traffic (bukan cuma label).
    """
    try:
        from app.services.relay_store import get_effective_use_proxy

        return bool(get_effective_use_proxy())
    except (ImportError, AttributeError, TypeError, ValueError):
        return bool(USE_PROXY)


def is_proxy_layer_active(use_proxy_override: Optional[bool] = None) -> bool:
    """True bila lapisan proxy boleh dipakai untuk request ini."""
    try:
        if use_proxy_override is not None:
            if not bool(use_proxy_override):
                return False
        elif not _effective_use_proxy_env():
            return False
        if not _global_enabled:
            return False
        ensure_loaded()
        with _store_lock:
            return any(
                isinstance(_proxies.get(pid), dict) and _proxies[pid].get("enabled")
                for pid in _proxies_order
            )
    except (TypeError, ValueError, AttributeError):
        return False


def is_global_enabled() -> bool:
    ensure_loaded()
    with _store_lock:
        return bool(_global_enabled)


def list_proxies_masked() -> List[Dict[str, Any]]:
    """Daftar proxy TANPA password (aman untuk dashboard)."""
    ensure_loaded()
    out: List[Dict[str, Any]] = []
    with _store_lock:
        for pid in list(_proxies_order):
            e = _proxies.get(pid)
            if not isinstance(e, dict):
                continue
            out.append({
                "id": pid,
                "scheme": e.get("scheme"),
                "host": e.get("host"),
                "port": e.get("port"),
                "username": e.get("username") or "",
                "has_auth": bool(e.get("password")),
                "enabled": bool(e.get("enabled", True)),
                "source": e.get("source", "file"),
                "display": display_proxy_url(e),
                "penalized": _is_proxy_penalized_locked(pid),
                "flagged": _is_proxy_flagged(pid),
            })
    return out


def _is_proxy_penalized_locked(pid: str) -> bool:
    try:
        return time.time() < _proxy_penalty.get(pid, 0.0)
    except (TypeError, ValueError, AttributeError):
        return False


def _is_proxy_penalized(pid: str) -> bool:
    with _proxy_penalty_lock:
        return time.time() < _proxy_penalty.get(pid, 0.0)


def _is_proxy_flagged(pid: str) -> bool:
    """True bila egress IP proxy sedang di-flag upstream (403)."""
    try:
        with _proxy_flagged_lock:
            return time.time() < _proxy_flagged.get(pid, 0.0)
    except (TypeError, ValueError, AttributeError):
        return False


def _is_proxy_sidelined(pid: str) -> bool:
    """True bila proxy disisihkan karena alasan apa pun (DOWN atau FLAGGED)."""
    try:
        return _is_proxy_penalized(pid) or _is_proxy_flagged(pid)
    except (TypeError, ValueError, AttributeError):
        return False


def _clear_proxy_flap(pid: str) -> None:
    """Hapus hitungan blip transport (dipakai saat koneksi TERBUKTI sehat —
    mis. request via proxy itu mendapat respons upstream, walau 403)."""
    try:
        with _proxy_flap_lock:
            _proxy_flap.pop(pid, None)
    except (TypeError, ValueError, AttributeError):
        pass


def _mark_proxy_failed(pid: str, cooldown: Optional[float] = None) -> None:
    """Sisihkan proxy karena TRANSPORT gagal (connect/timeout/429).

    - `cooldown` eksplisit -> dipakai apa adanya (backward-compat, mis. test).
    - `None` -> toleransi blip: kegagalan transport PERTAMA dalam jendela
      (_FLAP_WINDOW_S) hanya disisihkan sebentar (_FLAP_FIRST_COOLDOWN_S),
      bukan langsung PROXY_COOLDOWN penuh. Blip tunggal warp yang flaky tidak
      lagi "langsung cooldown lama padahal test OK". Kegagalan BERUNTUN
      (proxy benar-benar down) tetap memakai cooldown penuh.
    """
    try:
        pid = str(pid or "")
    except (TypeError, ValueError, AttributeError):
        return
    if not pid:
        return
    try:
        if cooldown is not None:
            try:
                until = time.time() + max(0.0, float(cooldown))
            except (TypeError, ValueError):
                until = time.time() + PROXY_COOLDOWN
        else:
            now = time.time()
            try:
                with _proxy_flap_lock:
                    # Prune entri basi agar dict tidak tumbuh tanpa batas.
                    for k in [k for k, v in _proxy_flap.items()
                              if now - float(v[1] if isinstance(v, (list, tuple)) else 0.0) > _FLAP_WINDOW_S]:
                        _proxy_flap.pop(k, None)
                    prev = _proxy_flap.get(pid)
                    if isinstance(prev, (list, tuple)) and len(prev) == 2:
                        count = int(prev[0]) + 1
                    else:
                        count = 1
                    _proxy_flap[pid] = (count, now)
            except (TypeError, ValueError, AttributeError):
                count = 2
            if count <= 1:
                until = now + _FLAP_FIRST_COOLDOWN_S
                _log("RELAY", f"proxy blip 1x {pid[:8]}: sisihkan {_FLAP_FIRST_COOLDOWN_S:.0f}s saja (bukan full cooldown)")
            else:
                try:
                    base = float(PROXY_COOLDOWN)
                except (TypeError, ValueError):
                    base = 60.0
                until = now + base
    except (TypeError, ValueError):
        until = time.time() + PROXY_COOLDOWN
    with _proxy_penalty_lock:
        now = time.time()
        for k in [k for k, v in _proxy_penalty.items() if v <= now]:
            _proxy_penalty.pop(k, None)
        _proxy_penalty[pid] = until


def mark_proxy_failed(pid: str, cooldown: Optional[float] = None) -> None:
    try:
        _mark_proxy_failed(str(pid or ""), cooldown)
    except (TypeError, ValueError, AttributeError):
        pass


def _mark_proxy_flagged_locked(pid: str, cooldown: Optional[float] = None) -> float:
    """Tandai egress IP proxy di-flag upstream (403). Return `until`.

    Dipanggil di bawah _proxy_flagged_lock. Sekaligus menghapus hitungan blip
    transport: 403 MEMBUKTIKAN koneksi proxy sehat (handshake + respons OK),
    jadi tidak boleh dihitung sebagai kegagalan transport.
    """
    now = time.time()
    for k in [k for k, v in _proxy_flagged.items() if v <= now]:
        _proxy_flagged.pop(k, None)
    try:
        span = max(0.0, float(PROXY_403_COOLDOWN if cooldown is None else cooldown))
    except (TypeError, ValueError):
        span = 180.0
    until = now + span
    _proxy_flagged[pid] = until
    return until


def mark_proxy_flagged(conn_url: str, cooldown: Optional[float] = None) -> None:
    """Sisihkan proxy karena UPSTREAM menolak egress IP-nya (403 FreeTierError).

    BUKAN vonis "proxy rusak": koneksi via proxy terbukti sehat (test OK),
    yang di-flag adalah IP egress-nya. No-op aman, tidak pernah melempar.
    """
    try:
        pid = pid_for_connection(conn_url or "")
        if not pid:
            return
        with _proxy_flagged_lock:
            until = _mark_proxy_flagged_locked(pid, cooldown)
        _clear_proxy_flap(pid)
        try:
            _log("RELAY", f"proxy FLAGGED {display_proxy_url(conn_url)}: egress IP ditolak upstream 403 "
                          f"(proxy SEHAT/test OK) -> disisihkan {max(0, int(until - time.time()))}s, pakai egress lain")
        except (TypeError, ValueError):
            pass
    except (TypeError, ValueError, AttributeError):
        pass


def reset_proxy_state() -> Dict[str, int]:
    """Hapus semua cooldown/flag + reset round-robin. Return jumlah dibersihkan."""
    global _proxy_index
    with _proxy_penalty_lock:
        n = len(_proxy_penalty)
        _proxy_penalty.clear()
    with _proxy_flagged_lock:
        f = len(_proxy_flagged)
        _proxy_flagged.clear()
    with _proxy_flap_lock:
        _proxy_flap.clear()
    with _proxy_index_lock:
        _proxy_index = 0
    try:
        _log("RELAY", f"proxy state direset via monitor (cooldown={n}, flagged={f})")
    except (TypeError, ValueError):
        pass
    return {"penalty_cleared": n, "flagged_cleared": f}


def add_proxy(
    scheme: str,
    host: str,
    port: Any,
    username: str = "",
    password: str = "",
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Tambah proxy. Return (entry_masked, error). Tidak pernah melempar."""
    try:
        ensure_loaded()
        scheme_n = _normalize_proxy_scheme(scheme)
        if not scheme_n:
            return None, "scheme harus salah satu: socks5, socks5h, http, https"
        host_n = (host or "").strip().lower().rstrip(".")
        if not host_n or len(host_n) > 253:
            return None, "host tidak valid"
        if not _HOST_RE.match(host_n) and not _is_ip_literal(host_n):
            return None, "host tidak valid"
        try:
            port_n = int(port)
        except (TypeError, ValueError):
            return None, "port harus 1-65535"
        if not (1 <= port_n <= 65535):
            return None, "port harus 1-65535"
        user_n = (username or "").strip()[:128]
        pwd_n = (password or "")[:256]
        if ("@" in user_n or ":" in user_n) and user_n:
            # ':' di username merusak parsing URL; tolak eksplisit.
            if ":" in user_n:
                return None, "username tidak boleh mengandung ':'"
        if any(c in pwd_n for c in ("@", " ", "\n", "\r")) and False:
            pass  # password boleh karakter apa pun (di-URL-encode httpx)
        pid = _proxy_id_for(scheme_n, host_n, port_n, user_n)
        with _store_lock:
            if pid in _proxies:
                return None, "proxy sudah ada (duplikat scheme/host/port/username)"
            if len(_proxies_order) >= PROXY_MAX_COUNT:
                return None, f"batas {PROXY_MAX_COUNT} proxy tercapai"
            _proxies[pid] = {
                "id": pid,
                "scheme": scheme_n,
                "host": host_n,
                "port": port_n,
                "username": user_n,
                "password": pwd_n,
                "enabled": True,
                "source": "file",
            }
            _proxies_order.append(pid)
            _save_file_locked()
            entry = dict(_proxies[pid])
        _log("RELAY", f"proxy ditambah {display_proxy_url(entry)}")
        masked = {k: v for k, v in entry.items() if k != "password"}
        masked["has_auth"] = bool(pwd_n)
        masked["display"] = display_proxy_url(entry)
        return masked, None
    except (TypeError, ValueError, AttributeError) as exc:
        return None, f"input tidak valid: {exc}"


def remove_proxy(pid: str) -> bool:
    """Hapus proxy + tutup client pooled-nya. Env-seed ikut terhapus sesi ini
    (muncul lagi saat restart — didokumentasikan di log)."""
    try:
        ensure_loaded()
        pid = (pid or "").strip()
        if not pid:
            return False
        with _store_lock:
            entry = _proxies.pop(pid, None)
            if entry is None:
                return False
            try:
                _proxies_order.remove(pid)
            except ValueError:
                pass
            conn = proxy_connection_url(entry) if isinstance(entry, dict) else ""
            _save_file_locked()
        # Tutup pooled client di luar lock (I/O).
        if conn:
            try:
                from app.core.http_client import _close_proxy_client
                import asyncio as _asyncio
                try:
                    loop = _asyncio.get_running_loop()
                    loop.create_task(_close_proxy_client(conn))
                except RuntimeError:
                    _asyncio.run(_close_proxy_client(conn))
            except (ImportError, AttributeError, TypeError, ValueError, RuntimeError, OSError):
                pass
        with _proxy_penalty_lock:
            _proxy_penalty.pop(pid, None)
        with _proxy_flagged_lock:
            _proxy_flagged.pop(pid, None)
        _clear_proxy_flap(pid)
        return True
    except (TypeError, ValueError, AttributeError):
        return False


def set_proxy_enabled(pid: str, enabled: bool) -> bool:
    try:
        ensure_loaded()
        pid = (pid or "").strip()
        with _store_lock:
            entry = _proxies.get(pid)
            if not isinstance(entry, dict):
                return False
            entry["enabled"] = bool(enabled)
            _save_file_locked()
            return True
    except (TypeError, ValueError, AttributeError):
        return False


def set_global_enabled(enabled: bool) -> bool:
    global _global_enabled
    try:
        ensure_loaded()
        with _store_lock:
            _global_enabled = bool(enabled)
            _save_file_locked()
            return bool(_global_enabled)
    except (TypeError, ValueError, AttributeError):
        return False


def seed_warp_pool(
    base_port: int = 40001, count: int = 10, host: str = "127.0.0.1",
    scheme: str = "socks5h",
) -> Dict[str, Any]:
    """Satu-klik tambah pool warp-socks lokal (40001-40010). Idempoten:
    yang sudah ada dilewati. Return {added, skipped}."""
    ensure_loaded()
    added = 0
    skipped = 0
    try:
        count = max(1, min(int(count), 32))
    except (TypeError, ValueError):
        count = 10
    try:
        base_port = int(base_port)
    except (TypeError, ValueError):
        base_port = 40001
    host = (host or "127.0.0.1").strip() or "127.0.0.1"
    for i in range(count):
        port = base_port + i
        if not (1 <= port <= 65535):
            skipped += 1
            continue
        entry, err = add_proxy(scheme, host, port)
        if entry is not None:
            added += 1
        else:
            skipped += 1
    _log("RELAY", f"proxy seed warp pool: added={added} skipped={skipped}")
    return {"added": added, "skipped": skipped}


def proxy_batch_for_request() -> List[Dict[str, Any]]:
    """Daftar proxy ENABLED untuk SATU request, titik awal dirotasi.

    Sehat dulu, disisihkan (DOWN transport / FLAGGED 403) di akhir (tetap
    cadangan). Return list entry FULL (termasuk password, untuk koneksi) —
    HANYA dipakai server-side, tidak pernah diserialisasi ke dashboard.
    """
    global _proxy_index
    try:
        ensure_loaded()
        with _store_lock:
            ids = [pid for pid in list(_proxies_order)
                   if isinstance(_proxies.get(pid), dict) and _proxies[pid].get("enabled")]
            snapshot = [dict(_proxies[pid]) for pid in ids]
        if not snapshot:
            return []
        n = len(snapshot)
        with _proxy_index_lock:
            start = _proxy_index % n
            _proxy_index += 1
        ordered = [snapshot[(start + i) % n] for i in range(n)]
        healthy = [e for e in ordered if not _is_proxy_sidelined(str(e.get("id")))]
        sidelined = [e for e in ordered if _is_proxy_sidelined(str(e.get("id")))]
        return healthy + sidelined
    except (TypeError, ValueError, AttributeError, ZeroDivisionError):
        return []


# ---- Target expansion (dipakai semua generator streaming + upstream) ----
# Target 2-tuple lama: (url, headers). Target 3-tuple baru:
# (url, headers, proxy_conn_url | None). Helper menerima list campuran
# 2/3-tuple dan mengembalikan 3-tuple seragam agar unpacking aman.

def _as_triple(target: Any) -> Optional[Tuple[str, Dict[str, Any], Optional[str]]]:
    try:
        if isinstance(target, (list, tuple)):
            if len(target) == 3:
                return (str(target[0]), dict(target[1] or {}), target[2])
            if len(target) == 2:
                return (str(target[0]), dict(target[1] or {}), None)
        return None
    except (TypeError, ValueError, AttributeError):
        return None


def expand_targets_with_proxy(
    targets: List[Any],
    use_proxy: Optional[bool] = None,
) -> List[Tuple[str, Dict[str, Any], Optional[str]]]:
    """Sisipkan proxy-direct SEBELUM direct mentah, relay tak tersentuh.

    - `targets` = [relay...] + [direct?] (2-tuple lama).
    - Bila lapisan proxy nonaktif / kosong / use_proxy=False -> kembalikan
      3-tuple ekuivalen TANPA menambah target (perilaku lama identik).
    - Bila aktif: tiap direct (tanpa `x-relay-target`) diganti
      [(direct, proxy1), ..., (direct, proxyN), (direct, None)].
    - Relay (dengan `x-relay-target`) TIDAK PERNAH lewat proxy lokal
      (relay sudah punya egress sendiri; proxying ke Vercel hanya menambah
      hop + risiko 504).
    """
    try:
        triples: List[Tuple[str, Dict[str, Any], Optional[str]]] = []
        for t in targets or []:
            conv = _as_triple(t)
            if conv is not None:
                triples.append(conv)
        if use_proxy is False:
            return triples
        if not is_proxy_layer_active(use_proxy):
            return triples
        batch = proxy_batch_for_request()
        if not batch:
            return triples
        out: List[Tuple[str, Dict[str, Any], Optional[str]]] = []
        for url, headers, existing_proxy in triples:
            try:
                is_relay = "x-relay-target" in (headers or {})
            except (TypeError, AttributeError):
                is_relay = False
            if is_relay or existing_proxy:
                out.append((url, headers, existing_proxy))
                continue
            # Direct -> kembangkan via proxy pool.
            for entry in batch:
                try:
                    conn = proxy_connection_url(entry)
                except (TypeError, ValueError, AttributeError):
                    continue
                if not conn:
                    continue
                out.append((url, dict(headers), conn))
            out.append((url, headers, None))
        return out
    except (TypeError, ValueError, AttributeError):
        # Fail-closed ke perilaku lama: kembalikan apa adanya sebagai triple.
        try:
            return [t for t in (_as_triple(x) for x in (targets or [])) if t is not None]
        except (TypeError, ValueError):
            return []


async def test_proxy_connection(
    proxy_conn_url: str,
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """Test satu proxy: GET ipify via proxy. Return {ok, ip, latency_ms}."""
    started = time.time()
    try:
        timeout_s = PROXY_TEST_TIMEOUT if timeout is None else max(1.0, min(float(timeout), 60.0))
    except (TypeError, ValueError):
        timeout_s = PROXY_TEST_TIMEOUT
    disp = display_proxy_url(proxy_conn_url)
    try:
        # Client FRESH per test (tidak memakai pool) agar proxy buruk tidak
        # meracuni koneksi pooled + timeout pendek untuk dashboard.
        async with httpx.AsyncClient(
            proxy=proxy_conn_url,
            timeout=httpx.Timeout(connect=timeout_s, read=timeout_s, write=timeout_s, pool=timeout_s),
        ) as client:
            resp = await client.get("https://api.ipify.org?format=json", headers={"Accept": "application/json"})
            latency = int((time.time() - started) * 1000)
            if resp.status_code != 200:
                return {"ok": False, "ip": None, "latency_ms": latency,
                        "display": disp, "error": f"HTTP {resp.status_code}"}
            try:
                data = resp.json()
                ip = data.get("ip") if isinstance(data, dict) else None
            except (ValueError, TypeError, AttributeError):
                ip = None
            if not ip:
                return {"ok": False, "ip": None, "latency_ms": latency,
                        "display": disp, "error": "respons tanpa IP"}
            return {"ok": True, "ip": ip, "latency_ms": latency, "display": disp, "error": None}
    except ImportError as exc:
        # socksio belum terpasang.
        return {"ok": False, "ip": None, "latency_ms": int((time.time() - started) * 1000),
                "display": disp, "error": f"socksio belum terpasang: {exc}"}
    except httpx.TimeoutException:
        return {"ok": False, "ip": None, "latency_ms": int((time.time() - started) * 1000),
                "display": disp, "error": f"timeout >{timeout_s:.0f}s"}
    except httpx.ConnectError as exc:
        return {"ok": False, "ip": None, "latency_ms": int((time.time() - started) * 1000),
                "display": disp, "error": f"connect gagal: {str(exc)[:120]}"}
    except httpx.ProxyError as exc:
        return {"ok": False, "ip": None, "latency_ms": int((time.time() - started) * 1000),
                "display": disp, "error": f"proxy error: {str(exc)[:120]}"}
    except httpx.RequestError as exc:
        return {"ok": False, "ip": None, "latency_ms": int((time.time() - started) * 1000),
                "display": disp, "error": f"{type(exc).__name__}: {str(exc)[:120]}"}
    except Exception as exc:  # noqa: BLE001 — test tak boleh melempar
        return {"ok": False, "ip": None, "latency_ms": int((time.time() - started) * 1000),
                "display": disp, "error": f"{type(exc).__name__}: {str(exc)[:120]}"}


async def test_proxy_by_id(pid: str) -> Dict[str, Any]:
    """Test proxy dari store berdasarkan id (termasuk kredensial)."""
    try:
        ensure_loaded()
        with _store_lock:
            entry = _proxies.get((pid or "").strip())
            entry = dict(entry) if isinstance(entry, dict) else None
        if entry is None:
            return {"ok": False, "ip": None, "latency_ms": 0, "error": "proxy tidak ditemukan"}
        conn = proxy_connection_url(entry)
        if not conn:
            return {"ok": False, "ip": None, "latency_ms": 0, "error": "konfigurasi proxy rusak"}
        result = await test_proxy_connection(conn)
        result["id"] = entry.get("id")
        return result
    except (TypeError, ValueError, AttributeError) as exc:
        return {"ok": False, "ip": None, "latency_ms": 0, "error": str(exc)[:200]}


def get_proxy_overview() -> Dict[str, Any]:
    """Ringkasan untuk dashboard + /v1/props (tanpa password)."""
    try:
        ensure_loaded()
        proxies = list_proxies_masked()
        eff_use_proxy = _effective_use_proxy_env()
        with _proxy_penalty_lock:
            now = time.time()
            penalized_ids = [k for k, v in _proxy_penalty.items() if v > now]
        with _proxy_flagged_lock:
            now2 = time.time()
            flagged_ids = [k for k, v in _proxy_flagged.items() if v > now2]
        return {
            "enabled_global": is_global_enabled() and eff_use_proxy,
            "use_proxy_env": eff_use_proxy,
            "use_proxy_file": is_global_enabled(),
            "count": len(proxies),
            "enabled_count": sum(1 for p in proxies if p.get("enabled")),
            "penalized_count": len(penalized_ids),
            "penalized_ids": penalized_ids,
            "flagged_count": len(flagged_ids),
            "flagged_ids": flagged_ids,
            "proxies": proxies,
        }
    except (TypeError, ValueError, AttributeError):
        try:
            eff = _effective_use_proxy_env()
        except (TypeError, ValueError, AttributeError):
            eff = False
        return {"enabled_global": False, "use_proxy_env": eff,
                "count": 0, "enabled_count": 0, "penalized_count": 0,
                "penalized_ids": [], "proxies": []}


def resolve_use_proxy(request_value: Optional[bool] = None) -> bool:
    """Nilai efektif use_proxy untuk satu request.

    - Eksplisit False -> selalu False (kill-switch per-request).
    - Eksplisit True -> True (expand tetap no-op bila pool kosong).
    - None -> ikut saklar efektif website/env (file toggle dicek di expand).
    Tidak pernah melempar.
    """
    try:
        if request_value is not None:
            return bool(request_value)
        return bool(_effective_use_proxy_env())
    except (TypeError, ValueError, AttributeError):
        return False


def pid_for_connection(conn_url: str) -> str:
    """Cari proxy id dari connection URL (cocokkan tanpa password bila perlu).

    Return "" bila tidak ketemu. Tidak pernah melempar.
    """
    try:
        ensure_loaded()
        cleaned = (conn_url or "").strip()
        if not cleaned:
            return ""
        with _store_lock:
            for pid, entry in _proxies.items():
                if not isinstance(entry, dict):
                    continue
                try:
                    if proxy_connection_url(entry) == cleaned:
                        return str(pid)
                except (TypeError, ValueError, AttributeError):
                    continue
            # Fallback: cocokkan scheme/host/port saja (auth mungkin berubah).
            try:
                from urllib.parse import urlparse as _up
                want = _up(cleaned)
                for pid, entry in _proxies.items():
                    if not isinstance(entry, dict):
                        continue
                    if (
                        str(entry.get("scheme") or "").lower() == (want.scheme or "").lower()
                        and str(entry.get("host") or "").lower() == ((want.hostname or "").lower())
                        and int(entry.get("port") or 0) == (want.port or 0)
                    ):
                        return str(pid)
            except (TypeError, ValueError, AttributeError):
                pass
        return ""
    except (TypeError, ValueError, AttributeError):
        return ""


def mark_proxy_conn_failed(conn_url: str, cooldown: Optional[float] = None) -> None:
    """Tandai proxy (dicari via connection URL) masuk cooldown. No-op aman."""
    try:
        pid = pid_for_connection(conn_url or "")
        if pid:
            _mark_proxy_failed(pid, cooldown)
    except (TypeError, ValueError, AttributeError):
        pass


def unpack_target(target: Any) -> Tuple[str, Dict[str, Any], Optional[str]]:
    """Unpack target 2-tuple lama / 3-tuple baru -> (url, headers, proxy)."""
    try:
        conv = _as_triple(target)
        if conv is not None:
            url, headers, proxy = conv
            if proxy is not None:
                proxy = str(proxy) if proxy else None
            return (url, dict(headers or {}), proxy)
    except (TypeError, ValueError, AttributeError):
        pass
    try:
        # Fallback defensif: jangan crash loop generator.
        return (str(target), {}, None)
    except (TypeError, ValueError):
        return ("", {}, None)

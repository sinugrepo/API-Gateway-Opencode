"""Admission control upstream + atribusi 429.

Modul ini menangani DUA masalah yang berbeda, dan sengaja TIDAK menangani
masalah ketiga.

1. 429 yang BISA DIHINDARI  -> token bucket (bagian `upstream_token`).
   Gateway mengirim request secepat klien datang. Saat 429, ia merotasi ke
   relay/proxy berikutnya. Bila batas upstream ternyata per-akun (bukan
   per-IP), rotasi tidak menemukan kuota baru: ia hanya MELIPATGANDAKAN
   jumlah request ke bucket yang sama, sehingga 429 justru bertahan lebih
   lama. Token bucket membatasi laju di sisi kita, bukan menemukannya lewat
   429.

2. 429 yang TIDAK BISA DIHINDARI -> harus bisa DIJELASKAN
   (bagian `record_429` + `snapshot`).
   Setiap 429 dicatat: target, route (relay/proxy/direct), tag sesi, model,
   sumber (Vercel vs OpenCode), dan Retry-After. Polanya lalu diklasifikasi:
   menempel ke satu target (IP/relay), satu sesi, atau menyebar ke semua
   (kuota akun). Tanpa data ini operator hanya menebak, dan tebakan yang
   salah berujung pada rotasi yang memperburuk keadaan.

3. Identitas request (header, sesi, TLS) TIDAK disentuh di sini.
   Modul ini hanya mengatur LAJU dan MEMBACA pola. Menyamarkan identitas
   agar lolos klasifikasi server adalah hal lain, dan bukan bagian dari
   modul ini.

4. Rotasi sesi saat sesi tertentu terus 429 (bagian `session_rotation_state`
   + `should_rotate_session`). Upstream menghitung limit per-sesi (bukan
   hanya per-IP): rotasi IP saja tidak menemukan kuota baru jika sesi itu
   sendiri sudah dipanaskan. Mekanisme ini melacak jumlah 429 beruntun per
   tag sesi dalam satu request; setelah threshold tercapai, caller diberi
   tahu untuk memicu fresh-session rotation (session + prompt_cache_key
   baru, rotasi ulang semua target). Berbeda dari fresh-session 403
   (satu upaya terakhir): ini berulang, tiap sesi baru yang juga 429
   beruntun bisa memicu rotasi lagi hingga batas total.
"""
import random
import threading
import time
from collections import Counter, deque
from typing import Any, Deque, Dict, List, Optional

from app.core.config import (
    RATE_GUARD_ENABLED,
    RATE_LIMIT_BACKOFF,
    UPSTREAM_429_SESSION_ROTATIONS,
    UPSTREAM_429_SESSION_THRESHOLD,
    UPSTREAM_429_WINDOW,
    UPSTREAM_BACKOFF_MAX,
    UPSTREAM_BURST,
    UPSTREAM_MAX_429_ROTATIONS,
    UPSTREAM_MAX_RPS,
)


# ==================== 1. ADMISSION CONTROL ====================

class _TokenBucket:
    """Token bucket RPS tanpa lock.

    Aman tanpa lock karena seluruh blok kritis di `acquire()` berjalan tanpa
    `await` di tengahnya: coroutine hanya bisa diinterupsi pada titik await,
    jadi baca-ubah-tulis `tokens` bersifat atomik terhadap event loop.
    (Satu proses = satu event loop; tidak dipakai lintas thread.)
    """

    __slots__ = ("rate", "capacity", "tokens", "updated", "waited", "wait_total")

    def __init__(self, rate: float, burst: int) -> None:
        self.rate = max(0.0, float(rate))
        self.capacity = max(1.0, float(burst))
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self.waited = 0
        self.wait_total = 0.0

    async def acquire(self) -> float:
        """Ambil satu token; kembalikan lama menunggu (detik)."""
        if self.rate <= 0:
            return 0.0
        import asyncio

        started = time.monotonic()
        while True:
            now = time.monotonic()
            elapsed = now - self.updated
            if elapsed > 0:
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.updated = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                waited = now - started
                if waited > 0.001:
                    self.waited += 1
                    self.wait_total += waited
                return waited
            # Tidur seperlunya saja; batas 0.5s membuat perubahan RPS runtime
            # (atau shutdown) tidak tertahan tidur panjang.
            deficit = 1.0 - self.tokens
            await asyncio.sleep(min(max(deficit / self.rate, 0.005), 0.5))


_bucket: Optional[_TokenBucket] = None

_bucket_lock = threading.Lock()


def _get_bucket() -> _TokenBucket:
    global _bucket
    with _bucket_lock:
        if _bucket is None:
            _bucket = _TokenBucket(UPSTREAM_MAX_RPS, UPSTREAM_BURST)
        return _bucket


async def upstream_token() -> float:
    """Gate satu attempt upstream. No-op bila RATE_GUARD_ENABLED=false.

    Dipanggil TEPAT SEBELUM setiap POST ke upstream (relay, proxy, maupun
    direct) sehingga total laju request keluar — berapa pun jumlah target
    yang dirotasi — dibatasi satu bucket global.
    """
    _bump("attempts")
    if not RATE_GUARD_ENABLED:
        return 0.0
    return await _get_bucket().acquire()


def admission_state() -> Dict[str, Any]:
    """Ringkasan konfigurasi + statistik tunggu token bucket."""
    bucket = _get_bucket()
    waited = bucket.waited
    return {
        "enabled": bool(RATE_GUARD_ENABLED),
        "max_rps": bucket.rate,
        "burst": bucket.capacity,
        "tokens_available": round(bucket.tokens, 2),
        "throttled_attempts": waited,
        "avg_wait_s": round(bucket.wait_total / waited, 3) if waited else 0.0,
    }


# ==================== 2. BACKOFF ====================

def backoff_delay(attempt: int, retry_after: Optional[float] = None) -> float:
    """Jeda sebelum attempt berikutnya: full-jitter, menghormati Retry-After.

    Full jitter (uniform 0..ceiling) dipakai supaya N request yang kena 429
    pada saat bersamaan tidak bangun pada milidetik yang sama lalu menabrak
    upstream serentak — pola itu sendiri yang memperpanjang 429.

    `Retry-After` upstream menang bila ada dan wajar (<= UPSTREAM_BACKOFF_MAX),
    karena itu angka yang otoritatif: menunggu lebih cepat hanya memicu 429
    berikutnya, menunggu jauh lebih lama membuang latensi tanpa manfaat.
    """
    if retry_after is not None and retry_after > 0:
        return min(float(retry_after), UPSTREAM_BACKOFF_MAX)
    base = max(0.0, float(RATE_LIMIT_BACKOFF))
    if base <= 0:
        return 0.0
    ceiling = min(UPSTREAM_BACKOFF_MAX, base * (2 ** max(0, int(attempt) - 1)))
    return random.uniform(0.0, ceiling)


# ==================== 3. RECORDER + ATRIBUSI ====================

_MAX_EVENTS = 300

_events: Deque[Dict[str, Any]] = deque(maxlen=_MAX_EVENTS)

_counters: Counter = Counter()

_lock = threading.Lock()


def _bump(key: str, amount: int = 1) -> None:
    with _lock:
        _counters[key] += amount


def _host_of(target: str) -> str:
    """Ambil host dari URL target agar relay/proxy tidak dihitung per-path."""
    raw = (target or "").strip()
    if not raw:
        return "-"
    if "://" in raw:
        raw = raw.split("://", 1)[1]
    raw = raw.split("/", 1)[0]
    return raw.split("@")[-1] or "-"


def record_429(
    *,
    route: str,
    target: str,
    session: str = "",
    model: str = "",
    attempt: int = 0,
    source: str = "unknown",
    retry_after: Optional[float] = None,
) -> None:
    """Catat satu 429 beserta identitas jalur yang menghasilkannya.

    `source` berasal dari `_classify_rate_limit()` (vercel / upstream /
    unknown). Perbedaan ini penting: 429 dari Vercel adalah kuota invokasi
    platform relay, 429 dari OpenCode adalah kuota API — keduanya butuh
    tindakan yang sama sekali berbeda.
    """
    if not RATE_GUARD_ENABLED:
        return
    now = time.time()
    host = _host_of(target)
    session_tag = (session or "-").strip() or "-"
    event = {
        "ts": now,
        "route": route or "?",
        "target": host,
        "session": session_tag,
        "model": model or "-",
        "attempt": int(attempt),
        "source": source or "unknown",
        "retry_after": round(float(retry_after), 2) if retry_after else None,
    }
    with _lock:
        _events.append(event)
        _counters["total"] += 1
        _counters[f"source:{event['source']}"] += 1
        _counters[f"route:{event['route']}"] += 1
        _counters[f"target:{host}"] += 1
        _counters[f"session:{session_tag}"] += 1
        _counters[f"model:{event['model']}"] += 1


def rotations_exhausted(rate_limit_hits: int) -> bool:
    """True bila budget rotasi-akibat-429 untuk satu request sudah habis.

    Rotasi lintas target hanya berguna bila limit bersifat per-IP. Setelah N
    target berbeda sama-sama menjawab 429, hipotesis per-IP sudah terbantah
    dan attempt berikutnya hanya menambah beban ke bucket yang sama. Berhenti
    lebih awal = 429 yang lebih pendek, bukan kegagalan yang lebih panjang.
    """
    if not RATE_GUARD_ENABLED:
        return False
    if UPSTREAM_MAX_429_ROTATIONS <= 0:
        return False
    return int(rate_limit_hits) >= UPSTREAM_MAX_429_ROTATIONS


# ==================== 4. SESSION ROTATION ON 429 ====================

# Sesi tertentu (termasuk sesi "stabil" per-fingerprint) bisa dipanaskan
# hingga limit per-sesinya habis — rotasi IP/relay saja tidak menemukan
# kuota baru. Mekanisme ini melacak jumlah 429 beruntun per tag sesi dalam
# SATU request klien; setelah threshold tercapai, caller memicu fresh
# session rotation (session + prompt_cache_key baru, rotasi ulang semua
# target). Setiap sesi baru yang juga 429 beruntun bisa memicu rotasi lagi,
# hingga batas total UPSTREAM_429_SESSION_ROTATIONS habis.
#
# State per-request (bukan global) karena sesi yang dipanaskan bersifat
# temporal: request berikutnya mungkin memakai sesi berbeda lagi. Key =
# session tag (10 char pertama ID, lihat _oc_session_tag).

class SessionRotationState:
    """Pelacak 429 beruntun per tag sesi dalam satu request.

    Dipakai oleh loop 429 di streaming/responses_bridge: setiap kali
    `note_429(tag)` dipanggil, hitungan untuk tag itu bertambah.
    `should_rotate(tag)` True bila tag terdeteksi panas (>= threshold)
    dan budget rotasi global belum habis.
    """

    __slots__ = ("per_session_hits", "rotations_done")

    def __init__(self) -> None:
        self.per_session_hits: Counter = Counter()
        self.rotations_done: int = 0

    def note_429(self, session_tag: str) -> None:
        tag = (session_tag or "-").strip() or "-"
        self.per_session_hits[tag] += 1

    def should_rotate(self, session_tag: str) -> bool:
        if UPSTREAM_429_SESSION_THRESHOLD <= 0 or UPSTREAM_429_SESSION_ROTATIONS <= 0:
            return False
        tag = (session_tag or "-").strip() or "-"
        return (
            self.per_session_hits[tag] >= UPSTREAM_429_SESSION_THRESHOLD
            and self.rotations_done < UPSTREAM_429_SESSION_ROTATIONS
        )

    def mark_rotated(self) -> None:
        self.rotations_done += 1
        # Reset hitungan per-sesi: sesi baru sekarang jadi kandidat.
        self.per_session_hits.clear()


def make_session_rotation_state() -> SessionRotationState:
    """Buat satu state per-request (aman dipanggil dari generator mana pun)."""
    return SessionRotationState()


def classify(window: Optional[float] = None) -> Dict[str, Any]:
    """Klasifikasi pola 429 dalam jendela waktu -> dugaan sumber limit.

    Urutan pemeriksaan disengaja: sumber platform (Vercel) diperiksa lebih
    dulu karena 429 semacam itu tidak ada hubungannya dengan OpenCode sama
    sekali, sehingga pola target/sesi tidak relevan.
    """
    span = float(UPSTREAM_429_WINDOW if window is None else window)
    now = time.time()
    with _lock:
        events = [e for e in _events if now - e["ts"] <= span]
    total = len(events)
    if total == 0:
        return {
            "verdict": "none",
            "label": "Belum ada 429 pada jendela ini",
            "confidence": "n/a",
            "evidence": {},
            "recommendation": (
                "Tidak ada data. Kalau klien tetap menerima 429, pastikan "
                "RATE_GUARD_ENABLED=true dan traffic memang melewati gateway."
            ),
            "window_s": span,
            "samples": 0,
        }

    sources = Counter(e["source"] for e in events)
    targets = Counter(e["target"] for e in events)
    sessions = Counter(e["session"] for e in events)
    routes = Counter(e["route"] for e in events)

    def share(counter: Counter, key: str) -> float:
        return counter.get(key, 0) / total

    top_target, top_target_n = targets.most_common(1)[0]
    top_session, top_session_n = sessions.most_common(1)[0]
    evidence = {
        "distinct_targets": len(targets),
        "distinct_sessions": len(sessions),
        "top_target": {"value": top_target, "count": top_target_n, "share": round(share(targets, top_target), 2)},
        "top_session": {"value": top_session, "count": top_session_n, "share": round(share(sessions, top_session), 2)},
        "by_route": dict(routes),
        "by_source": dict(sources),
    }

    if share(sources, "vercel") >= 0.6:
        return {
            "verdict": "relay-platform",
            "label": "Kuota platform relay (Vercel), bukan OpenCode",
            "confidence": "high",
            "evidence": evidence,
            "recommendation": (
                "429 datang dari Vercel (batas invokasi fungsi / TOO_MANY_REQUESTS), "
                "bukan dari API OpenCode. Menambah proxy tidak menolong. Yang "
                "menolong: menambah relay, menurunkan UPSTREAM_MAX_RPS, atau "
                "menaikkan plan relay."
            ),
            "window_s": span,
            "samples": total,
        }

    if len(targets) == 1 and total >= 3:
        return {
            "verdict": "target-scoped",
            "label": f"Menempel ke satu egress ({top_target})",
            "confidence": "high" if total >= 6 else "medium",
            "evidence": evidence,
            "recommendation": (
                "Semua 429 berasal dari satu target. Ini pola limit per-IP: "
                "egress itulah yang dihitung upstream. Rotasi ke target lain "
                "memang solusi yang benar untuk pola ini, dan cooldown yang ada "
                "sudah menanganinya."
            ),
            "window_s": span,
            "samples": total,
        }

    if len(sessions) == 1 and total >= 3 and len(targets) >= 2:
        return {
            "verdict": "session-scoped",
            "label": f"Menempel ke satu sesi ({top_session})",
            "confidence": "high" if total >= 6 else "medium",
            "evidence": evidence,
            "recommendation": (
                "429 mengikuti satu sesi melintasi beberapa egress, jadi yang "
                "dihitung upstream adalah sesi/percakapan itu — bukan IP. "
                "Periksa percakapan yang memakai tag tersebut (payload besar, "
                "loop tool, atau riwayat panjang)."
            ),
            "window_s": span,
            "samples": total,
        }

    if len(targets) >= 3 and len(sessions) >= 2:
        return {
            "verdict": "account-scoped",
            "label": "Menyebar ke banyak target dan banyak sesi",
            "confidence": "medium" if total < 10 else "high",
            "evidence": evidence,
            "recommendation": (
                "429 muncul di banyak egress DAN banyak sesi sekaligus. Ini "
                "pola kuota tingkat akun/key, bukan per-IP. Rotasi tidak akan "
                "menyembuhkan; turunkan UPSTREAM_MAX_RPS, kurangi konkurensi, "
                "atau tambah kuota."
            ),
            "window_s": span,
            "samples": total,
        }

    return {
        "verdict": "mixed",
        "label": "Pola belum konklusif",
        "confidence": "low",
        "evidence": evidence,
        "recommendation": (
            "Sampel masih sedikit atau tersebar tidak merata. Biarkan traffic "
            "berjalan lebih lama sebelum menyimpulkan; lihat daftar 429 terakhir "
            "di bawah untuk target dan sesi yang terlibat."
        ),
        "window_s": span,
        "samples": total,
    }


def snapshot() -> Dict[str, Any]:
    """Payload untuk dashboard /monitor/api/ratelimit."""
    with _lock:
        counters = dict(_counters)
        recent = list(_events)[-20:]
    recent.reverse()

    attempts = max(1, int(counters.get("attempts", 0)))
    total = int(counters.get("total", 0))

    def group(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
        rows = [
            {"key": k.split(":", 1)[1], "count": v}
            for k, v in counters.items()
            if k.startswith(prefix)
        ]
        rows.sort(key=lambda r: r["count"], reverse=True)
        return rows[:limit]

    return {
        "admission": admission_state(),
        "attempts": int(counters.get("attempts", 0)),
        "rate_limited": total,
        "rate_limited_pct": round(100.0 * total / attempts, 1),
        "budget": {
            "max_429_rotations": UPSTREAM_MAX_429_ROTATIONS,
            "backoff_max_s": UPSTREAM_BACKOFF_MAX,
        },
        "by_source": group("source:"),
        "by_route": group("route:"),
        "by_target": group("target:"),
        "by_session": group("session:"),
        "by_model": group("model:"),
        "classification": classify(),
        "recent": recent,
    }


def reset() -> Dict[str, int]:
    """Kosongkan recorder + statistik admission (tombol Reset di dashboard)."""
    global _bucket
    with _lock:
        removed = len(_events)
        _events.clear()
        _counters.clear()
    with _bucket_lock:
        _bucket = None
    return {"events_cleared": removed}

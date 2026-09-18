"""Central configuration (env vars + pure helpers)."""
import os
import secrets
from urllib.parse import urlparse

APP_VERSION = "2.0.0"


# Do not hard-code secrets in source code. Set OPENCODE_API_KEY in your service environment.
API_KEY = os.getenv("OPENCODE_API_KEY", "public")


OPENCODE_URL = os.getenv(
    "OPENCODE_URL",
    "https://opencode.ai/zen/v1/chat/completions",
)


OPENCODE_MODELS_URL = os.getenv(
    "OPENCODE_MODELS_URL",
    "https://opencode.ai/zen/v1/models",
)


# Muse Spark (dan model Responses-only lain) TIDAK dilayani lewat
# /chat/completions (upstream jawab 500 Internal server error) melainkan lewat
# Responses API. Endpoint proxy /v1/responses meneruskan body mentah ke sini.
OPENCODE_RESPONSES_URL = os.getenv(
    "OPENCODE_RESPONSES_URL",
    "https://opencode.ai/zen/v1/responses",
)


# Substring model (koma-dipisah, case-insensitive) yang HANYA dilayani lewat
# Responses API. Chat request ke model ini dijembatani otomatis
# (chat -> Responses -> chat) agar klien chat-only seperti Hermes tetap jalan.
RESPONSES_ONLY_MODELS = [
    s.strip().lower()
    for s in os.getenv("RESPONSES_ONLY_MODELS", "muse-spark").split(",")
    if s.strip()
]


def _is_responses_only_model(model: str) -> bool:
    """True bila model hanya tersedia lewat Responses API (bukan chat)."""
    name = (model or "").lower()
    return any(marker in name for marker in RESPONSES_ONLY_MODELS)


# Free tier upstream WAJIB menerima header sesi ala CLI, kalau tidak request
# ditolak: FreeTierError ("free tier can only be used in OpenCode").
# Hermes lama tidak mengirimnya (baru ada di build pasca PR NousResearch
# #101864), jadi proxy menyuntikkannya agar terlihat seperti opencode CLI:
#   x-opencode-client: cli | x-opencode-project: global
#   x-opencode-session: ses_<12 hex timestamp + 14 base62> (stabil 1/conversation)
#   x-opencode-request: msg_<12 hex timestamp + 14 base62> (unik per POST)
# Format persis mengikuti `opencode-session.md` §4 (Go createID).
# Satu sesi dipakai bersama untuk semua upstream attempt dalam SATU request
# klien (relay + fallback direct). Bila klien sudah mengirim header tersebut,
# nilai klien dihormati (diutamakan) agar sesi per-percakapan tetap stabil.
OPENCODE_CLIENT_NAME = os.getenv("OPENCODE_CLIENT_NAME", "cli")


OPENCODE_PROJECT = os.getenv("OPENCODE_PROJECT", "global")


# Opsional: paksa satu session ID statis global (bila di-set, selalu dipakai
# ketika klien tidak mengirim x-opencode-session sendiri).
OPENCODE_SESSION_ID = os.getenv("OPENCODE_SESSION_ID", "").strip()


# The client selects the upstream model from the live OpenCode model list.
# Keep this empty by default: never silently route requests to a hard-coded model.
MODEL = os.getenv("MODEL", "").strip()


# Relay URLs for round-robin. Set RELAY_URLS as comma-separated, or RELAY_URL for single.
# Penting: setiap URL = deployment Vercel yang BEDA, sehingga egress IP-nya
# berbeda ke upstream OpenCode. Semakin banyak relay -> setiap request
# bergantian memakai relay yang berbeda dan peluang kena 429 bersama mengecil.
_DEFAULT_RELAYS = [
    "https://relay-01-wmgc.vercel.app/api/relay",
    "https://relay-01-deas.vercel.app/api/relay",
    "https://relay-02-rzve.vercel.app/api/relay",
    "https://relay-03-xsgt.vercel.app/api/relay",
    "https://relay-04-cgbt.vercel.app/api/relay",
    "https://relay-05-vxeu.vercel.app/api/relay",
    "https://relay-06-amac.vercel.app/api/relay",
    "https://relay-07-nhxb.vercel.app/api/relay",
    "https://relay-08-pjxj.vercel.app/api/relay",
    "https://relay-09-icqy.vercel.app/api/relay",
    "https://relay-10-jjgx.vercel.app/api/relay"
]


def _normalize_relay_url(raw: str) -> str:
    """Normalisasi entri relay: terima hostname telanjang maupun URL penuh.

    - Backslash/whitespace dibersihkan (typo umum saat paste).
    - Tanpa skema -> https://.
    - Tanpa path (mis. "relay-x.vercel.app") -> /api/relay, karena semua
      deployment relay memakai endpoint fungsi yang sama.
    """
    cleaned = (raw or "").strip().rstrip("\\/").strip()
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


RELAY_URLS_ENV = os.getenv("RELAY_URLS", "")


if RELAY_URLS_ENV:
    RELAY_URLS = [
        normalized
        for u in RELAY_URLS_ENV.split(",")
        for normalized in [_normalize_relay_url(u)]
        if normalized
    ]
else:
    single = os.getenv("RELAY_URL")
    normalized_single = _normalize_relay_url(single or "")
    if normalized_single:
        RELAY_URLS = [normalized_single]
    else:
        RELAY_URLS = list(_DEFAULT_RELAYS)


if not RELAY_URLS:
    # Jangan pernah biarkan daftar relay kosong: _relay_batch_for_request()
    # melakukan modulo len(RELAY_URLS) dan default arg test_relay_connection
    # memakai RELAY_URLS[0] — keduanya crash (ZeroDivision/IndexError) bila kosong.
    RELAY_URLS = list(_DEFAULT_RELAYS)


USE_RELAY = os.getenv("USE_RELAY", "true").lower() == "true"


RELAY_FALLBACK = os.getenv("RELAY_FALLBACK", "true").lower() == "true"


# Timeout SEMANTIK: jumlah detik tanpa PROGRESS dari upstream (idle timeout),
# bukan batas total durasi stream. Stream yang terus mengirim byte boleh
# berjalan berapa pun lamanya; hanya stream yang macet (tidak ada data sama
# sekali selama REQUEST_TIMEOUT) yang dibunuh.
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "120"))


# Upstream rate-limit (429) handling. Saat upstream merespons 429, proxy
# melakukan retry dengan backoff (mengutamakan header `Retry-After` upstream
# bila ada). Setelah semua retry habis, proxy mengembalikan 429 yang "bersih"
# + header Retry-After, sehingga klien (Hermes/SDK) menunggu alih-alih
# membombardir ulang dan memicu spam exception.
RATE_LIMIT_RETRIES = int(os.getenv("RATE_LIMIT_RETRIES", "2"))


RATE_LIMIT_BACKOFF = float(os.getenv("RATE_LIMIT_BACKOFF", "2.0"))


# Berapa lama relay yang baru kena 429 di-skip dari rotasi (cooldown).
# Rate limit upstream (per-IP) JARANG pulih dalam hitungan detik — di log
# relay-fix sepanjang menit masih 429. Default 60s membuat request berikutnya
# langsung memakai relay yang sehat (relay panas disusulkan ke akhir urutan).
RATE_LIMIT_COOLDOWN = float(os.getenv("RATE_LIMIT_COOLDOWN", "60"))


# Bug spam-429 opencode khusus muse-spark: upstream kadang mengembalikan 429
# padahal belum limit. Untuk model Responses-only (muse-spark & co.), coba 1x
# lagi ke route/relay YANG SAMA (sleep backoff dulu) sebelum ganti route.
# Model lain tetap langsung rotasi agar tidak menambah latensi sia-sia.
SPURIOUS_429_SAME_ROUTE_RETRIES = int(os.getenv("SPURIOUS_429_SAME_ROUTE_RETRIES", "1"))


# Streaming TIDAK bypass relay secara default (false) agar IP upstream tetap
# ter-masking. CATATAN Edge (terbukti di log produksi): Vercel Edge WAJIB
# mengirim byte pertama dalam 25 detik; total streaming boleh sampai 300 detik
# HANYA bila syarat itu terpenuhi. Request konteks raksasa (TTFB upstream
# >25s) DIBUNUH platform dengan 504 sebelum byte pertama keluar — relay sehat
# pun terlihat mati. Solusi permanen ada di kode relay (flush header +
# heartbeat SEGERA sebelum fetch upstream). Set "true" hanya untuk debugging
# (mengirim SSE langsung ke OPENCODE_URL tanpa masking IP).
STREAM_BYPASS_RELAY = os.getenv("STREAM_BYPASS_RELAY", "false").lower() == "true"


# Ambang payload "konteks raksasa" (byte JSON). Di atas ambang ini, TTFB
# upstream wajar >25s sehingga 504 Edge adalah HASIL YANG DIHARAPKAN, bukan
# cacat relay: proxy tidak boleh menandai relay stream-broken (kalau ditandai,
# 2 request besar meracuni semua relay 30 menit dan traffic normal ikut jatuh
# ke direct), dan attempt relay non-stream dibatasi agar tidak membakar
# 25s x 11 relay sebelum fallback.
RELAY_GIANT_PAYLOAD_BYTES = int(os.getenv("RELAY_GIANT_PAYLOAD_BYTES", "32768"))


# Idle interval (in seconds) at which the streaming path yields an SSE
# comment line (`:\\n\\n`) to keep the connection alive. Several reverse
# proxies and CDNs (nginx default `proxy_read_timeout`, Cloudflare's
# free idle timeout, several corporate MITM boxes) will silently kill a
# response that carries no bytes for ~30-60 seconds. Emitting a no-op
# SSE comment every N seconds lets the proxy register activity on the
# socket without disturbing the client.
SSE_KEEPALIVE_INTERVAL = float(os.getenv("SSE_KEEPALIVE_INTERVAL", "5"))


HERMES_COMPAT = os.getenv("HERMES_COMPAT", "true").lower() == "true"


# Forward `reasoning_content` deltas to the client during the reasoning phase.
# Without this, long reasoning keeps the socket quiet (only `:\n\n` comments),
# which some clients (KiloCode, OpenAI SDK wrappers) treat as an idle timeout and
# abort mid-stream. When enabled, reasoning bytes flow so the socket stays alive.
REASONING_FORWARD = os.getenv("REASONING_FORWARD", "true").lower() == "true"


# Token usage tracking - persistent SQLite storage shared across restarts.
USAGE_DB_PATH = os.getenv("USAGE_DB_PATH", "./usage.db")


# This does not force a syntax on the model. It only prevents it from placing
# tool-call markup into human-readable prose and reminds it to keep shell
# arguments intact.
HERMES_TOOL_INSTRUCTION = (
    "When tools are available, use the API tool-calling mechanism instead of "
    "describing a tool invocation in normal assistant text. Do not expose "
    "DSML/XML tool markup to the user. Preserve all literal whitespace in "
    "tool arguments, especially shell commands, paths, flags, and JSON."
)


# Password for the monitoring dashboard. Change this in production.
MONITOR_PASSWORD = os.getenv("MONITOR_PASSWORD", "admin123")


MONITOR_SECRET = os.getenv(
    "MONITOR_SECRET",
    secrets.token_hex(32),
)


MONITOR_COOKIE_NAME = "monitor_token"


MONITOR_TOKEN_TTL = 86400  # 24 hours


# ==================== LIVE LOG BUFFER (dashboard) ====================
# Ring buffer in-memory untuk Live Logs di /monitor. Sengaja dibatasi
# (default 500 baris) agar memori tetap datar (~150KB) dan _log() tetap O(1).
# _log() tidak pernah block: append deque di bawah lock singkat, tanpa I/O.
LIVE_LOG_MAXLEN = int(os.getenv("LIVE_LOG_MAXLEN", "500"))


MODELS_CACHE_TTL_SECONDS = int(os.getenv("MODELS_CACHE_TTL_SECONDS", "300"))


# Berapa lama relay yang kena 403 FreeTierError di-skip dari rotasi.
# Berbeda dari 429 (kuota per-IP yang pulih cepat), 403 menandakan egress IP
# relay sedang di-flag upstream — flag semacam ini jarang pulih dalam
# hitungan detik. Relay yang "terlarang" disusulkan ke akhir urutan (tetap
# jadi cadangan), sehingga request berikutnya langsung memakai relay sehat.
RELAY_403_COOLDOWN = float(os.getenv("RELAY_403_COOLDOWN", "300"))


# Berapa lama relay yang timeout di-skip untuk request streaming.
# Default 30 menit: timeout platform Vercel bukan kondisi transien (limit
# eksekusi ~25 dtk tidak pulih sendiri), jadi menandai ulang tiap 5 menit
# hanya mengulang biaya ~25 dtk x N relay secara berkala.
RELAY_STREAM_BROKEN_COOLDOWN = float(os.getenv("RELAY_STREAM_BROKEN_COOLDOWN", "1800"))


# Berapa banyak relay yang boleh dicoba untuk SATU request streaming sebelum
# menyerah ke direct. Tanpa batas ini, 1 request streaming bisa mencoba 14
# relay x ~25 dtk (504 Vercel) = ~350 dtk hang sebelum direct — seperti di log
# (06->07->08->09->10->main2->... tiap 25 dtk). Default 2: total worst-case
# ~50 dtk, setelah itu direct. Relay yang timeout tetap di-mark stream-broken
# 1800s sehingga request berikutnya langsung direct tanpa biaya lagi.
# Set 0 = streaming langsung direct (tanpa coba relay sama sekali).
MAX_RELAY_STREAM_ATTEMPTS = int(os.getenv("MAX_RELAY_STREAM_ATTEMPTS", "2"))


# Timeout idle + read khusus CHAT-BRIDGE streaming (muse-spark & co.).
# Konteks panjang (input_items 100+ di Hermes) membuat TTFB upstream wajar
# 120-300 detik: model harus membaca ulang seluruh konteks tiap request.
# REQUEST_TIMEOUT global (120s) membunuh direct yang sebenarnya sehat
# (lihat log: DIRECT ReadTimeout setelah 120s padahal bukan rate-limit).
# Nilai ini dipakai sebagai batas idle-loop DAN read-timeout httpx per-request
# hanya di responses_to_chat_stream_generator — jalur lain tidak berubah.
BRIDGE_REQUEST_TIMEOUT = float(os.getenv("BRIDGE_REQUEST_TIMEOUT", "300"))


RELAY_STATUS_TIMEOUT = float(os.getenv("RELAY_STATUS_TIMEOUT", "8"))


# Bot massal rutin memindai path seperti /.env, /.aws/credentials,
# /terraform.tfstate*, /config.json.bak, dsb. (lihat access log 404 beruntun
# dari satu IP). Semua itu 404 dan tidak membocorkan apa pun, tapi berisik
# dan layak diblokir otomatis ala fail2ban — langsung di dalam app agar
# jalan di mana pun tanpa perlu akses root/firewall di server.
SCAN_GUARD_ENABLED = os.getenv("SCAN_GUARD_ENABLED", "true").lower() == "true"


# Berapa probe mencurigakan dalam jendela waktu -> IP di-ban sementara.
SCAN_GUARD_THRESHOLD = int(os.getenv("SCAN_GUARD_THRESHOLD", "8"))


SCAN_GUARD_WINDOW = float(os.getenv("SCAN_GUARD_WINDOW", "120"))


SCAN_GUARD_BAN_SECONDS = float(os.getenv("SCAN_GUARD_BAN_SECONDS", "3600"))


# Bila True, percayai X-Forwarded-For untuk IP klien (WAJIB True bila di
# belakang reverse proxy/CDN — kalau False, yang ke-ban justru IP proxy dan
# semua user ikut terblokir!). Default False (server terekspos langsung).
SCAN_GUARD_TRUST_PROXY = os.getenv("SCAN_GUARD_TRUST_PROXY", "false").lower() == "true"


# Batas memori: jumlah IP maksimum yang dilacak (terlama dibuang duluan).
SCAN_GUARD_MAX_IPS = int(os.getenv("SCAN_GUARD_MAX_IPS", "20000"))


# Batas ukuran body request (byte). Tanpa ini FastAPI mem-buffer body
# sembarang besar ke memori (DoS OOM via base64 raksasa). Dicek dari header
# Content-Length SEBELUM app tersentuh -> 413 cepat, streaming-safe.
# 8MB menutupi vision maksimal (3MB media + overhead JSON/tools) dengan
# headroom, namun jauh di bawah zona bahaya OOM.
MAX_REQUEST_BYTES = int(os.getenv("MAX_REQUEST_BYTES", str(8 * 1024 * 1024)))

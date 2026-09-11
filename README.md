# Hermes Gateway — OpenAI-Compatible API Gateway

Proxy FastAPI yang kompatibel OpenAI untuk upstream `opencode.ai/zen/v1`, dengan
relay round-robin Vercel (masking IP), bridge Chat → Responses untuk model
Responses-only (cth. `muse-spark`), tracking usage SQLite, dan dashboard
monitoring di `/monitor`.

Entry point: `main.py` → package `app/` (`app:create_app`).

## Fitur

- `POST /v1/chat/completions` — non-stream + SSE `stream=true`, tool-calling
  OpenAI + fallback parser DSML, vision (`image_url`/`file` → `input_image`/`input_file`),
  batas media ~3 MB → 400 jelas.
- `POST /v1/responses` (alias `/responses`) — pass-through mentah ke upstream
  Responses API untuk Muse Spark & sejenisnya.
- Bridge otomatis: model Responses-only yang diminta via chat dijembatani
  `chat → Responses → chat` agar klien chat-only tetap jalan.
- Relay: 11 deployment Vercel di-rotasi per-request, 429-cooldown 60 dtk,
  penanda stream-broken 1800 dtk untuk timeout platform, giant-payload guard
  (>32 KB / >100 item tidak menandai relay rusak), `MAX_RELAY_STREAM_ATTEMPTS=2`.
- Rate-limit: retry + backoff, hormati `Retry-After` upstream; 429 bersih +
  header `Retry-After` ke klien. Penanganan khusus spurious-429 muse-spark.
- Streaming stabil: `Accept-Encoding: identity`, SSE keepalive 5 dtk,
  `reasoning_content` diteruskan agar socket tidak idle.
- Usage: SQLite WAL (`usage.db`) per `request_id` + agregasi per model/period.
- Hardening: `BodyLimitMiddleware` 413 via `Content-Length` (>8 MB),
  `ScanGuardMiddleware` ala fail2ban untuk probe `/.env` dkk, HMAC cookie
  monitor, tanpa GZip (merusak SSE).
- Dashboard `/monitor`: KPI, grafik token/request, Recent Requests + Live Logs
  (SSE), Relay status/IPs, ScanGuard bans + Unban, Config mini.

## Syarat

- Python 3.14, `pip install -r requirements.txt`
- Dependensi: `fastapi`, `httpx`, `pydantic`, `uvicorn`
  (+ `uvloop` hanya Linux, di-skip otomatis di Windows)

## Jalankan

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
# dev saja: RELOAD=true uvicorn main:app --reload
```

`main.py` bila dijalankan langsung (`python main.py`) membaca `PORT`
(default 8000), `RELOAD` (default `false`), dan memakai `uvloop` bila ada.

## Konfigurasi (env)

| Var | Default | Keterangan |
|---|---|---|
| `OPENCODE_API_KEY` | `public` | Jangan hard-code; set di environment |
| `OPENCODE_URL` | `.../zen/v1/chat/completions` | Upstream chat |
| `OPENCODE_RESPONSES_URL` | `.../zen/v1/responses` | Upstream responses |
| `OPENCODE_MODELS_URL` | `.../zen/v1/models` | Daftar model |
| `RESPONSES_ONLY_MODELS` | `muse-spark` | Substring model yang hanya via Responses |
| `MODEL` | `` (kosong) | Tidak ada default diam-diam; klien wajib kirim model |
| `RELAY_URLS` / `RELAY_URL` | 11 relay bawaan | Koma-dipisah; hostname telanjang dinormalisasi ke `/api/relay` |
| `USE_RELAY` / `RELAY_FALLBACK` | `true` | Relay + fallback direct |
| `REQUEST_TIMEOUT` | `120` | Idle timeout non-bridge (dtk) |
| `BRIDGE_REQUEST_TIMEOUT` | `300` | Idle/read timeout bridge muse-spark |
| `RATE_LIMIT_RETRIES/BACKOFF/COOLDOWN` | `2/2.0/60` | Retry 429 upstream |
| `MAX_RELAY_STREAM_ATTEMPTS` | `2` | Batas coba relay per stream (`0`=direct) |
| `RELAY_STREAM_BROKEN_COOLDOWN` | `1800` | Skip relay timeout-platform |
| `STREAM_BYPASS_RELAY` | `false` | `true` hanya debug (tanpa masking IP) |
| `USAGE_DB_PATH` | `./usage.db` | SQLite WAL |
| `MONITOR_PASSWORD` | `admin123` | Wajib ganti di produksi |
| `MONITOR_SECRET` | acak per-start | HMAC cookie; set statis bila multi-proses |
| `ENFORCE_MONITOR_PASSWORD` | `false` | `true` = tolak start bila password default |
| `SCAN_GUARD_*` | `true/8/120/3600/false` | `TRUST_PROXY=true` wajib bila di belakang proxy |
| `MAX_REQUEST_BYTES` | `8388608` | 413 cepat sebelum buffer OOM |
| `LIVE_LOG_MAXLEN` | `500` | Ring buffer live-log |
| `PORT` / `RELOAD` | `8000` / `false` | Server |

## Endpoint

Inferensi:

- `POST /v1/chat/completions` — OpenAI chat (bridge otomatis bila Responses-only)
- `POST /v1/responses`, `POST /responses` — Responses pass-through
- `GET /v1/models`, `GET /models`, `GET /v1/models/{id}`, `GET /api/tags`,
  `GET /api/v1/models`, `POST /api/show` — discovery (Ollama-compatible stub)
- `GET /health`, `GET /version`, `GET /v1/props`, `GET /relay/status`
- `GET /v1/usage?period=today|3h|6h|1d|7d|30d` atau `?start=YYYY-MM-DD&end=YYYY-MM-DD`,
  `GET /v1/usage/periods`

Monitor (cookie `monitor_token`, 24 jam):

- Halaman: `GET /monitor/login`, `POST /monitor/login`, `GET /monitor`,
  `GET /monitor/logout`
- API: `/monitor/api/health`, `/monitor/api/session`,
  `/monitor/api/usage`, `/monitor/api/usage/history`,
  `/monitor/api/requests/recent?limit=20`, `/monitor/api/relay`,
  `/monitor/api/props`, `/monitor/api/security`,
  `POST /monitor/api/security/unban`, `/monitor/api/logs`,
  `POST /monitor/api/logs/clear`, `POST /monitor/api/relays/reset`,
  `/monitor/api/logs/stream` (SSE)

Dashboard memisahkan jalur cepat (usage/history) dari jalur lambat
(relay probe ~8 dtk) agar ganti period tidak memblokir.

## Struktur

```text
main.py                  # entry uvicorn + delegasi legacy import
app/__init__.py          # create_app, lifespan, middleware, router
app/core/                # config, schemas, errors, sse, http_client,
                         # logging_utils, error_handlers
app/security/            # body_limit, scan_guard, monitor_auth
app/services/            # relay, upstream, streaming, responses_bridge,
                         # opencode, models_cache, tools_dsml, usage
app/routes/              # chat, responses_api, misc, usage_routes, monitor
app/web/templates/       # login.html, dashboard.html (vanilla, tanpa build)
test/                    # test_long_stream_fixes.py, test_live_requests.py
plans/                   # docs lokal, di-gitignore
usage.db*                # runtime SQLite, di-gitignore
```

## Test

```bash
python -m compileall -q app main.py test
python test/test_long_stream_fixes.py   # offline, 34 case, harus ALL PASSED
python test/test_live_requests.py       # live: ASGI in-process → relay Vercel
                                        # + opencode.ai; kontrak 200 / 429-bersih /
                                        # EMPTY_RESPONSE; butuh internet
```

## Catatan produksi

- Ganti `MONITOR_PASSWORD`; pertimbangkan `ENFORCE_MONITOR_PASSWORD=true`.
- Set `SCAN_GUARD_TRUST_PROXY=true` bila di belakang reverse proxy/CDN.
- Jangan ekspos langsung tanpa firewall/bind `127.0.0.1`/reverse proxy —
  tidak ada auth klien (sengaja demi Hermes).
- Jangan tambah GZip (buffer SSE) atau `workers>1` tanpa uji SQLite dulu.

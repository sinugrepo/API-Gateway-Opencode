# Sinug Gateway — OpenAI-Compatible API Gateway

Proxy FastAPI yang kompatibel OpenAI untuk upstream `opencode.ai/zen/v1`, dengan
relay round-robin Vercel (masking IP), bridge Chat → Responses untuk model
Responses-only (cth. `muse-spark`), tracking usage SQLite, dan dashboard
monitoring di `/monitor`.

Entry point: `main.py` → package `app/` (`app:create_app`).

## Fitur

- `POST /v1/chat/completions` — non-stream + SSE `stream=true`, tool-calling
  OpenAI + fallback parser DSML, vision (`image_url`/`file` → `input_image`/`input_file`),
  batas media ~3 MB → 400 jelas. `tool_choice` klien (`none`/`required`/named)
  dikoersi ke `"auto"` bila tools ada (provider Console menolak selain auto
  dengan 400) atau di-drop bila tidak ada tools — berlaku di semua jalur.
- `POST /v1/responses` (alias `/responses`) — pass-through mentah ke upstream
  Responses API untuk Muse Spark & sejenisnya.
- Bridge otomatis: model Responses-only yang diminta via chat dijembatani
  `chat → Responses → chat` agar klien chat-only tetap jalan.
- Relay: 11 deployment Vercel di-rotasi per-request, 429-cooldown 60 dtk,
  penanda stream-broken 1800 dtk untuk timeout platform, giant-payload guard
  (>32 KB / >100 item tidak menandai relay rusak), `MAX_RELAY_STREAM_ATTEMPTS=2`.
  Request yang relay-nya mustahil sukses (thinking xhigh spark, konteks
  raksasa — TTFB wajar >25s limit Vercel) langsung direct-first, relay tetap
  fallback (`DIRECT_FIRST_SLOW`); tanpa ini tiap request membuang ~50 dtk
  churn relay dan klien seperti Hermes reconnect dalam loop stall.
- Rate-limit: retry + backoff, hormati `Retry-After` upstream; 429 bersih +
  header `Retry-After` ke klien. Penanganan khusus spurious-429 muse-spark.
- Free-tier 403: relay yang kena 403 di-cooldown 300 dtk; bila SEMUA target
  (termasuk direct) 403 pra-payload — yang di-flag identitas request, bukan
  IP — rotasi PENUH sekali lagi dengan SATU pasangan (session,
  prompt_cache_key) baru yang konsisten (`FORBIDDEN_FRESH_SESSION_RETRY`,
  `FORBIDDEN_RETRY_DELAY`), kecuali payload membawa replay `encrypted_content`
  (identitas wajib stabil). Fingerprint §8 opencode-session.md ditegakkan di
  semua jalur (kuartet tools, stream:true, store:false, key stabil, xhigh).
- Streaming stabil: `Accept-Encoding: identity`, SSE keepalive 5 dtk,
  `reasoning_content` diteruskan agar socket tidak idle — termasuk reasoning
  yang datang utuh via `response.output_item.done` (tanpa summary delta);
  `BRIDGE-SUMMARY`/`EMPTY-STREAM` mencatat rincian `types=` per tipe event.
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

`.env` di root project otomatis dimuat saat start (tanpa `python-dotenv`);
contoh siap salin di `.env.example`. Environment yang sudah ada menang atas `.env`.

| Var | Default | Keterangan |
|---|---|---|
| `GATEWAY_API_KEYS` / `GATEWAY_API_KEY` / `API_KEYS` | `` (kosong = terbuka) | **Auth klien gateway** (koma-dipisah, digabung). Terisi = `/v1/*` + `/api/*` + `/relay/status` wajib `Authorization: Bearer <key>` / `x-api-key: <key>` / `?api_key=`; kosong = mode terbuka (lokal saja). BEDA dari `OPENCODE_API_KEY` |
| `OPENCODE_API_KEY` | `public` | Key upstream server-to-server; jangan dibagikan ke klien |
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
| `MODEL_CONTEXT_OVERRIDES_JSON` | `` (kosong) | Override window konteks, cth. `'{"my-model": 500000, "qwen*": 1000000}'` (exact + `prefix*`, case-insensitive) |
| `MODEL_ENDPOINT_OVERRIDES_JSON` | `` (kosong) | Override kategori endpoint native (`chat`/`responses`/`messages`), cth. `'{"my-model": "responses"}'` (exact + `prefix*`) |
| `RESPONSES_REVERSE_BRIDGE` | `true` | `false` = model non-Responses via `/v1/responses` ditolak 400 bersih (tanpa bridge balik) |
| `FORBIDDEN_FRESH_SESSION_RETRY` | `true` | `false` = matikan upaya terakhir sesi-baru saat semua target 403 |
| `FORBIDDEN_RETRY_DELAY` | `2.0` | Jeda (dtk) sebelum upaya terakhir sesi-baru |
| `DIRECT_FIRST_SLOW` | `false` | `true` = direct dulu untuk thinking xhigh/giant (relay fallback); default relay-first agar rotasi 11 IP maksimal menemukan egress bersih saat flagging dinamis |
| `PORT` / `RELOAD` | `8000` / `false` | Server |

## Endpoint

Inferensi:

- `POST /v1/chat/completions` — OpenAI chat (bridge otomatis bila Responses-only).
  Vision: part `image_url`/`file` OpenAI, `image`+`source` Anthropic
  (base64/url), dan `input_image`/`input_file` Responses-style yang nyasar
  via chat semuanya dikonversi ke `input_image`/`input_file`; `detail`
  (`auto`/`low`/`high`) diteruskan. `/v1/models` mengiklankan
  `modalities: ["text","image"]` + `supports_vision` untuk muse-spark,
  gpt, gemini, grok, claude, qwen, glm, kimi, minimax, deepseek-vision.
- `POST /v1/responses`, `POST /responses` — Responses pass-through untuk
  model Responses-native (muse-spark/gpt/grok); model chat/messages-native
  (mimo/deepseek/glm/kimi/minimax/claude/qwen/...) otomatis dijembatani
  balik lewat pipeline chat (reverse bridge) — SEMUA model jalan di KEDUA
  endpoint. Kategori native per model di `app/services/model_endpoints.py`.
- `GET /v1/models`, `GET /models`, `GET /v1/models/{id}`, `GET /api/tags`,
  `GET /api/v1/models`, `POST /api/show` — discovery (Ollama-compatible stub).
  Setiap entri OpenAI diperkaya `context_length` / `context_window` /
  `max_input_tokens` / `max_context_length` dari tabel kanonis
  (`app/services/model_context.py`; muse-spark = 1.048.576 terverifikasi
  https://dev.meta.ai/docs/models).
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
- Model Test (panel dashboard, manual saja — tidak ada auto-test):
  `GET /monitor/api/models` (daftar free + context window),
  `POST /monitor/api/models/test`
  (`{model, prompt, max_tokens}` → `{ok, status, latency_ms, output, usage, error}`;
  selalu 200 agar Test-All tidak berhenti di 429 pertama).
  Test All meminta konfirmasi browser dulu; endpoint dibatasi 30 hit / 5 mnt
  (429 + `retry_after` bila lewat) dan setiap uji dicatat di live-log
  (`model-test OK/FAIL/RATE-LIMITED` + IP pemanggil) agar batch misterius
  bisa ditelusuri. Tidak ada timer/refresh yang memicu inferensi.

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
                         # chat_bridge (reverse bridge), opencode,
                         # models_cache, model_context, model_endpoints,
                         # tools_dsml, usage
app/routes/              # chat, responses_api, misc, usage_routes, monitor
app/web/templates/       # login.html, dashboard.html (vanilla, tanpa build)
test/                    # test_long_stream_fixes.py, test_live_requests.py,
                         # test_reverse_bridge.py, test_forbidden_fresh_retry.py,
                         # test_direct_first_slow.py, test_output_item_done.py,
                         # test_tool_choice_coerce.py
plans/                   # docs lokal, di-gitignore
usage.db*                # runtime SQLite, di-gitignore
```

## Test

```bash
python -m compileall -q app main.py test
python test/test_long_stream_fixes.py   # offline, 34 case, harus ALL PASSED
python test/test_reverse_bridge.py      # offline, 15 case (routing endpoint + reverse bridge)
python test/test_forbidden_fresh_retry.py  # offline, 7 case (retry sesi-baru all-403)
python test/test_direct_first_slow.py  # offline, 8 case (direct-first request lambat)
python test/test_tool_choice_coerce.py  # offline, 7 case (koersi tool_choice auto)
python test/test_output_item_done.py  # offline, 5 case (reasoning utuh done-event)
python test/test_live_requests.py       # live: ASGI in-process → relay Vercel
                                        # + opencode.ai; kontrak 200 / 429-bersih /
                                        # EMPTY_RESPONSE; butuh internet
```

## Catatan produksi

- Isi `GATEWAY_API_KEYS` di `.env` (lihat `.env.example`); tanpa ini gateway
  mode TERBUKA. Klien kirim `Authorization: Bearer <key>` (atau
  `x-api-key: <key>`). `/health`, `/version`, `/monitor` (cookie sendiri)
  tetap publik; `/v1/*`, `/api/*`, `/relay/status` diproteksi bila key diisi.
- Ganti `MONITOR_PASSWORD`; pertimbangkan `ENFORCE_MONITOR_PASSWORD=true`.
- Set `SCAN_GUARD_TRUST_PROXY=true` bila di belakang reverse proxy/CDN.
- Jangan ekspos langsung tanpa firewall/bind `127.0.0.1`/reverse proxy.
- Jangan tambah GZip (buffer SSE) atau `workers>1` tanpa uji SQLite dulu.

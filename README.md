# Sinug Gateway — OpenAI-Compatible API Gateway

Proxy FastAPI yang kompatibel OpenAI untuk upstream `opencode.ai/zen/v1`, dengan
relay round-robin Vercel (masking IP), pool proxy SOCKS/HTTP (egress tambahan),
bridge Chat → Responses untuk model Responses-only (cth. `muse-spark`),
tracking usage SQLite, dan dashboard `/monitor` yang terbagi menjadi
Monitoring / Konfigurasi / Uji Model. Relay, proxy, dan jalur egress
**dikonfigurasi penuh via website** (tanpa hardcoded, tanpa restart).

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
- Relay: pool deployment Vercel di-rotasi per-request, 429-cooldown 60 dtk,
  penanda stream-broken 1800 dtk untuk timeout platform, giant-payload guard
  (>32 KB / >100 item tidak menandai relay rusak), `MAX_RELAY_STREAM_ATTEMPTS=2`.
  Daftar relay + saklar `use_relay` / `relay_fallback` dikelola via website
  (tersimpan di `relays.json`); env (`RELAY_URLS`, `USE_RELAY`,
  `RELAY_FALLBACK`) hanya default awal. Vision tetap direct-first (biner base64
  rawan 413/504 relay). `DIRECT_FIRST_SLOW` deprecated (no-op).
- Egress (jalur keluar, diatur via website tanpa restart): `relay_first`
  (`relay → proxy → direct`, default, masking IP maksimal) atau `proxy_first`
  (`proxy → relay → direct`, pool proxy benar-benar dilewati tiap request).
  Matikan relay untuk mode proxy/direct murni; matikan fallback untuk relay-only.
- Outbound proxy: pool SOCKS5/HTTP untuk request DIRECT (round-robin per-request,
  relay tidak pernah lewat proxy; kosong = no-op, perilaku lama dipertahankan).
  Verifikasi via `api64.ipify.org` (IPv6, unik per node WARP V6ONLY) + trace
  `warp=on`; `api.ipify.org` itu IPv4-only (shared `104.28.x.x` sama di semua
  node WARP gratis — bukan bug, jangan dipakai menilai variasi IP). `socks5h`
  wajib (DNS via proxy agar AAAA ter-resolve di dalam container warp).
  Dikelola via website (`proxies.json`; password tidak pernah dikembalikan ke UI).
  Status per-proxy: `READY` / `DOWN` (transport gagal) / `FLAGGED` (egress IP
  ditolak upstream 403 — proxy SEHAT/test OK, yang di-flag IP-nya). Blip transport
  tunggal hanya disisihkan 15 dtk; gagal beruntun memakai `PROXY_COOLDOWN`;
  403 memakai `PROXY_403_COOLDOWN`. Per-request override `use_proxy` seperti
  `use_relay`.
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
- Dashboard `/monitor` — tiga halaman terpisah: **Monitoring** (KPI, grafik
  token/request, Recent Requests + Live Logs SSE, status egress ringkas, hasil
  probe relay — read-only), **Konfigurasi** (egress + preview rute, pool relay,
  pool proxy, ScanGuard, snapshot konfigurasi efektif), **Uji Model**.
- MCP Server (Streamable HTTP): `POST /mcp` (JSON-RPC `initialize` /
  `tools/list` / `tools/call`, single + batch, notifikasi → 202),
  `GET /mcp` discovery, `DELETE /mcp` no-op stateless, plus SSE legacy
  `GET /sse` + `POST /messages`. Tools: `chat` (semua model, muse-spark
  auto-bridge), `responses` (native spark/gpt/grok, reverse-bridge model lain),
  `list_models`, `gateway_props`. Auth sama seperti `/v1/*`.
- MCP tool passthrough: tools gaya MCP (`inputSchema`) dinormalisasi ke
  OpenAI (`parameters`) di `/v1/chat/completions` + `/v1/responses`
  (dua arah bridge, `strict` dipertahankan) — MCP servers di `opencode.json`
  tetap jalan termasuk via muse-spark. `tool_choice` tetap dikoersi `auto`
  (batasan provider Console).
- Skema rekursif MCP (`$ref` siklik gaya Pydantic, termasuk mutual A↔B)
  diputus otomatis sebelum wire (`{"type": "object"}`) karena provider
  Console menolaknya dengan 400 `Recursive JSON schemas are not currently
  supported`; `$ref` non-siklik dipertahankan utuh.
- Skema terlalu dalam (>10 level nesting, umum di tools MCP auto-generate)
  dipadatkan otomatis di bawah limit provider (400 `maximum nesting depth`),
  dengan `type` asal dipertahankan; skema tulisan-tangan (2-4 level) tak
  tersentuh.
- `additionalProperties` dalam bentuk apa pun (bool `false` maupun dict
  ber-skema — provider menolak key-nya, live: `analysis_profile` via spark)
  di-drop otomatis (400 `Invalid JSON schema`); `required` asli yang
  non-kosong (`required: []` ikut di-drop) dan `type` yang hilang
  (`properties`→object, `items`→array) dilengkapi tanpa mengubah makna.
- Error SSE `/v1/responses` kini diawali event Responses-valid
  `{"type":"error","sequence_number":0,"message":...}` agar klien ketat
  (Kilo Code) menampilkan pesan alih-alih `UnknownError`;
  chunk `{"error":...}` warisan tetap dikirim untuk kolektor internal.
- Log `FATAL-400` mencetak 500 char pertama + 300 char terakhir detail
  (bukan 200) agar pesan provider penuh terlihat untuk iterasi berikutnya,
  plus black-box recorder: wire `tools` persis yang dikirim + body upstream
  utuh tersimpan di `/tmp/sinug-fatal-400.json` (overwrite tiap kejadian)
  untuk diagnosis presisi tanpa menebak.
- Fail-fast 400 payload-error: 400 yang jelas salah payload (skema rekursif,
  schema invalid, tool tak dikenal, konteks kepanjangan) langsung dikembalikan
  tanpa merotasi 13 target (~25 dtk sia-sia + menandai proxy/relay sehat
  sebagai rusak). 400 replay `encrypted_content` tetap lewat auto-heal.
- Wire `/v1/responses` direct kini selengkap wire bridge yang terbukti lolos:
  `prompt_cache_key` stabil disintesis bila klien tidak mengirim (bagian
  fingerprint gate §8 `opencode-session.md`), `max_output_tokens` default
  65536, `temperature`/`top_p` null di-drop, dan tools MCP (`inputSchema` /
  bentuk chat) dinormalisasi ke flat responses ber-`parameters` — request
  MCP tidak lagi 403 FreeTier sementara request biasa lolos.

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
| `RELAY_URLS` / `RELAY_URL` | 11 relay bawaan | Default awal pool; hostname telanjang dinormalisasi ke `/api/relay`. Daftar efektif dikelola via website (`relays.json`) |
| `USE_RELAY` / `RELAY_FALLBACK` | `true` | Default saklar relay + fallback direct; override via website (null = ikut env) |
| `USE_PROXY` | `true` | Default saklar proxy; override via website (`relays.json`) + toggle file (`proxies.json`) |
| `OUTBOUND_PROXIES` | `` (kosong) | Seed awal pool proxy koma-dipisah (tanpa skema = `socks5h`); kelola via website |
| `PROXY_COOLDOWN` | `60` | Detik proxy DOWN (transport gagal beruntun) disisihkan; blip tunggal hanya 15 dtk |
| `PROXY_403_COOLDOWN` | `180` | Detik proxy FLAGGED (egress IP ditolak upstream 403) disisihkan |
| `PROXY_TEST_TIMEOUT` | `10` | Timeout tombol Test proxy di dashboard (dtk) |
| `PROXY_CONFIG_PATH` / `RELAY_CONFIG_PATH` | `./proxies.json` / `./relays.json` | File persistensi konfigurasi website (di-gitignore) |
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
| `DIRECT_FIRST_SLOW` | `false` | DEPRECATED no-op: relay-first selalu untuk non-vision; vision tetap direct-first |
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

MCP (auth sama seperti `/v1/*` bila `GATEWAY_API_KEYS` diisi):

- `POST /mcp` — Streamable HTTP JSON-RPC (`initialize` →
  `notifications/initialized` → `tools/list` → `tools/call`).
- `GET /mcp` — discovery (info server + daftar tools).
- `DELETE /mcp` — terminasi sesi (stateless: selalu OK).
- `GET /sse` + `POST /messages` — transport SSE legacy (klien MCP lama).
- `GET /.well-known/mcp` — discovery alternatif.

Contoh `opencode.json` (remote MCP + provider gateway):

```json
{
  "provider": {
    "sinug": {
      "npm": "@ai-sdk/openai-compatible",
      "options": { "baseURL": "http://127.0.0.1:8000/v1", "apiKey": "sk-gateway-anda" }
    }
  },
  "mcp": {
    "sinug-gateway": {
      "type": "remote",
      "url": "http://127.0.0.1:8000/mcp",
      "headers": { "Authorization": "Bearer sk-gateway-anda" }
    }
  }
}
```

Monitor (cookie `monitor_token`, 24 jam):

- Halaman: `GET /monitor/login`, `POST /monitor/login`, `GET /monitor`,
  `GET /monitor/logout`
- API: `/monitor/api/health`, `/monitor/api/session`,
  `/monitor/api/usage`, `/monitor/api/usage/history`,
  `/monitor/api/requests/recent?limit=20`, `/monitor/api/relay` (probe semua relay),
  `/monitor/api/props` (termasuk `relay_pool` + `egress_order`), `/monitor/api/security`,
  `POST /monitor/api/security/unban`, `/monitor/api/logs`,
  `POST /monitor/api/logs/clear`, `POST /monitor/api/relays/reset`,
  `/monitor/api/logs/stream` (SSE)
- Relay pool + egress (halaman Konfigurasi, tanpa restart):
  `GET /monitor/api/relays` (daftar + saklar efektif),
  `POST /monitor/api/relays` (`{url}` tambah),
  `POST /monitor/api/relays/remove` (`{id}`),
  `POST /monitor/api/relays/enable` (`{id, enabled}`),
  `POST /monitor/api/relays/config`
  (`{use_relay, relay_fallback, use_proxy, egress_order}` parsial; `null` = ikut env),
  `POST /monitor/api/relays/test` (`{id}` atau `{url}`, ad-hoc tanpa menyimpan)
- Outbound proxy (halaman Konfigurasi, tanpa restart):
  `GET /monitor/api/proxies` (ringkasan tanpa password),
  `POST /monitor/api/proxies` (tambah `{scheme, host, port, username?, password?}`),
  `POST /monitor/api/proxies/remove`, `POST /monitor/api/proxies/enable`,
  `POST /monitor/api/proxies/global`, `POST /monitor/api/proxies/test`
  (`{id}` atau ad-hoc), `POST /monitor/api/proxies/test-all`,
  `POST /monitor/api/proxies/reset`, `POST /monitor/api/proxies/seed-warp`
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
app/services/            # relay (+ relay_store: pool & egress runtime),
                         # outbound_proxy (pool SOCKS/HTTP + DOWN/FLAGGED),
                         # upstream, streaming, responses_bridge,
                         # chat_bridge (reverse bridge), opencode,
                         # models_cache, model_context, model_endpoints,
                         # tools_dsml, usage
app/routes/              # chat, responses_api, misc, usage_routes, monitor, mcp
app/web/templates/       # login.html, dashboard.html (vanilla, tanpa build;
                          # 3 view: Monitoring / Konfigurasi / Uji Model)
test/                    # test_long_stream_fixes.py, test_live_requests.py,
                          # test_reverse_bridge.py, test_forbidden_fresh_retry.py,
                          # test_direct_first_slow.py, test_output_item_done.py,
                          # test_tool_choice_coerce.py, test_outbound_proxy.py,
                          # test_mcp.py (offline, 43 case: normalisasi
                          # inputSchema + sanitizer rekursi + JSON-RPC + HTTP layer)
plans/                   # docs lokal, di-gitignore
usage.db*                # runtime SQLite, di-gitignore
proxies.json             # pool proxy website, di-gitignore
relays.json              # pool relay + egress website, di-gitignore
```

## Test

```bash
python -m compileall -q app main.py test
python test/test_mcp.py   # offline, 43 case (MCP server + passthrough + sanitizer + paritas + depth + strict + Kilo-compat + recorder)
python test/test_outbound_proxy.py   # offline, 8 case (pool proxy + flap/flag + expand)
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

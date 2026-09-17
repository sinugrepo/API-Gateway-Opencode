# OpenCode Session — Dari Mana & Cara POST ke Muse

## 1. Kesimpulan

`x-opencode-session: ses_...` **tidak didapat dari POST ke `opencode.ai`**.
ID dibuat **lokal oleh OpenCode CLI**, disimpan di SQLite lokal, lalu dikirim sebagai header ke Zen / MUSE ROUTER untuk affinity routing, prompt caching, dan korelasi billing.

> Tidak ada `POST /session` eksternal saat mulai. Yang ada hanya `INSERT` lokal atau `POST localhost:4096/session` (server lokal).

Bukti di mesin ini (`~/.local/share/opencode/opencode.db`, tabel `session`):

```
ses_f524da710ffeDgjRMLdA4vq6MJ | global | Test 1 2 3 message | 2026-09-17 04:49:01
```

ID ini sama persis dengan header yang terlihat di traffic ke `https://opencode.ai/zen/v1/responses`.

## 2. Request yang terlihat di network

```http
POST https://opencode.ai/zen/v1/responses
Authorization: Bearer public
User-Agent: opencode/1.18.30 ai-sdk/provider-utils/4.0.40 runtime/bun/1.3.14
x-opencode-client: cli
x-opencode-project: global
x-opencode-request: msg_0adb2593a001naxAlU7UCQlwGa
x-opencode-session: ses_f524da710ffeDgjRMLdA4vq6MJ
Content-Type: application/json
```

* URL = OpenCode Zen gateway, endpoint kompatibel OpenAI Responses API.
* `Bearer public` = token public untuk tier gratis (`muse-spark-1.3-contributor-free` dkk).
* `opencode/1.18.30` = versi CLI pengirim.

## 3. Source code penentu header

`packages/opencode/src/session/llm/request.ts` (~baris 188):

```ts
headers: {
  ...(input.model.providerID.startsWith("opencode")
    ? {
        ...(opencodeProjectID ? { "x-opencode-project": opencodeProjectID } : {}),
        "x-opencode-session": input.sessionID, // <- ID lokal
        "x-opencode-request": input.user.id,   // <- msg_...
        "x-opencode-client": input.flags.client,
        "User-Agent": USER_AGENT,
      }
    : {
        "x-session-affinity": input.sessionID,
        "X-Session-Id": input.sessionID,
        "User-Agent": USER_AGENT,
      }),
}
```

`input.sessionID` = ID session lokal. `input.user.id` = ID message lokal.

## 4. Format generator `ses_` / `msg_`

Reverse-engineer dari `kode-ai/providers/opencode/headers.go`:

* Total panjang suffix: 26 char = 12 hex + 14 base62.
* `base62 = 0-9A-Za-z`.
* Session: `descending=true`, request: `descending=false`.

```go
const idLength = 26
const idRandom = 14 // 26-12

func NewSessionID(t time.Time) (string, error) {
  id, _ := createID(true, t, 1)
  return "ses_" + id, nil
}
func NewRequestID(t time.Time) (string, error) {
  id, _ := createID(false, t, 1)
  return "msg_" + id, nil
}
func createID(descending bool, t time.Time, counter uint16) (string, error) {
  now := uint64(t.UnixMilli())*0x1000 + uint64(counter)
  if descending { now = ^now }
  var buf [8]byte
  binary.BigEndian.PutUint64(buf[:], now)
  random, _ := randomBase62(14)
  return fmt.Sprintf("%x%s", buf[2:], random), nil
}
```

### Generator Python (ekuivalen)

```python
import time, secrets, struct
base62='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
def rnd(n):
    return ''.join(base62[b % 62] for b in secrets.token_bytes(n))
def create(desc, cnt=1):
    ms=int(time.time()*1000)
    now=(ms*0x1000+cnt) & ((1<<64)-1)
    if desc:
        now=(~now) & ((1<<64)-1)
    b=struct.pack('>Q', now)[2:]
    return b.hex()+rnd(14)

ses='ses_'+create(True)
msg='msg_'+create(False)
print(ses, msg)
```

Contoh hasil valid:

```
ses_f5236e71bffeX30hIKB5N4EMwD
msg_0adc918e4001A877QyBqOFRt0r
```

## 5. Cara POST ke Muse dengan session generator

### 5.1 MUSE ROUTER (openai-compatible)

Config di `~/.config/opencode/config.json`:

```json
{
  "provider": {
    "muse": {
      "npm": "@ai-sdk/openai-compatible",
      "options": { "baseURL": "http://43.156.122.11:8000/v1" }
    }
  }
}
```

curl:

```bash
curl -i http://43.156.122.11:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer public" \
  -H "x-opencode-session: ses_f52374d06ffe6jB51MVjwL088x" \
  -H "x-opencode-request: msg_0adc8b2f9001WiWmXNEu9DRCAR" \
  -H "x-opencode-project: global" \
  -H "x-opencode-client: cli" \
  -H "User-Agent: opencode/1.18.31" \
  -d '{"model":"muse-spark-1.3-contributor-free","messages":[{"role":"user","content":"tes 1 2 3, balas dengan OK saja"}],"max_tokens":200}'
```

Hasil test: `200 OK` tapi `content:""` — router memotong reasoning internal.

### 5.2 Zen Responses API (pembanding, berhasil)

```bash
curl -i https://opencode.ai/zen/v1/responses \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer public" \
  -H "x-opencode-session: ses_f5236e71bffeX30hIKB5N4EMwD" \
  -H "x-opencode-request: msg_0adc918e4001A877QyBqOFRt0r" \
  -H "x-opencode-project: global" \
  -H "x-opencode-client: cli" \
  -H "User-Agent: opencode/1.18.31" \
  -d '{"model":"muse-spark-1.3-contributor-free","input":"tes 1 2 3, balas dengan OK saja","max_output_tokens":500}'
```

Hasil test:

```json
{
  "status": "completed",
  "model": "muse-spark-1.3-contributor-free",
  "output": [
    {"type": "reasoning", "status": "completed"},
    {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "OK"}]}
  ],
  "usage": {"input_tokens": 19, "output_tokens": 149, "total_tokens": 168}
}
```

### 5.3 Python lengkap

```python
import time, secrets, struct, json, urllib.request
base62='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
def rnd(n): return ''.join(base62[b%62] for b in secrets.token_bytes(n))
def create(desc, cnt=1):
    ms=int(time.time()*1000)
    now=(ms*0x1000+cnt) & ((1<<64)-1)
    if desc: now=(~now) & ((1<<64)-1)
    return struct.pack('>Q', now)[2:].hex()+rnd(14)
ses, msg = 'ses_'+create(True), 'msg_'+create(False)
headers={
 "Content-Type":"application/json",
 "Authorization":"Bearer public",
 "x-opencode-session":ses,
 "x-opencode-request":msg,
 "x-opencode-project":"global",
 "x-opencode-client":"cli",
 "User-Agent":"opencode/1.18.31",
}
for url, payload in [
 ("http://43.156.122.11:8000/v1/chat/completions",
  {"model":"muse-spark-1.3-contributor-free","messages":[{"role":"user","content":"tes 1 2 3"}],"max_tokens":200}),
 ("https://opencode.ai/zen/v1/responses",
  {"model":"muse-spark-1.3-contributor-free","input":"tes 1 2 3","max_output_tokens":500}),
]:
    req=urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=90) as r:
        print(r.status, r.read().decode()[:2000])
```

## 6. Cara verifikasi lokal tanpa network

```bash
# list session terbaru
python3 -c "import sqlite3,os; db=os.path.expanduser('~/.local/share/opencode/opencode.db'); con=sqlite3.connect(db); cur=con.cursor(); cur.execute('SELECT id, project_id, title, datetime(time_created/1000,\"unixepoch\") FROM session ORDER BY time_updated DESC LIMIT 5'); print('\n'.join(map(str,cur.fetchall())))"

# via server lokal (jika opencode serve jalan)
curl localhost:4096/session
```

## 7. Aturan pakai header

* `x-opencode-session` harus **stabil 1 ID per conversation** (untuk cache affinity). Jangan generate baru tiap message dalam 1 percakapan.
* `x-opencode-request` harus **unik per message** (`msg_...` baru tiap POST).
* `x-opencode-project` = `global` jika di luar git, atau hash root commit + cache di `.git/opencode` jika di dalam repo.
* Sejak pengumuman 3 Sep 2026, request tanpa `x-opencode-session` bisa ditolak / kehilangan optimasi cache.

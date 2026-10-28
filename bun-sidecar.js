// bun-sidecar.js — egress front untuk direct + WARP-localhost proxy.
//
// Tujuan: fingerprint TLS Bun native (persis OpenCode CLI) untuk semua
// egress yang keluar dari SERVER INI (direct + 10 container WARP Cloudflare
// di localhost). Relay Vercel TIDAK lewat sini: TLS-nya di sisi Vercel,
// tidak bisa di-fingerprint dari server kita.
//
// ALUR:  gateway Python  <->  [TCP 127.0.0.1:BUN_PROXY_PORT]  <->  Bun
//       (otak logika)                (tangan network)
//
// Kenapa WARP + Bun = fingerprint asli: WARP hanya memberi terowongan (TUN)
// ke internet — dia TIDAK menjalankan TLS handshake ke opencode.ai. Handshake
// tetap terjadi di server kita. Selama request itu di-route lewat socket
// WARP tetapi TLS di-handshake oleh runtime Bun, JA3/JA4 yang dilihat
// opencode.ai = fingerprint Bun (bukan OpenSSL/Python).
//
// PROTOKOL (plain-text line-delimited, aman di-pipe dari Python):
//   REQUEST  (gateway -> Bun), satu socket = satu request:
//     <METHOD> <target_url> <content_length> <proxy_spec>\n
//     <Header-Name>: <value>\n            (nol atau banyak baris)
//     \n                                     (empty line = header selesai)
//     <body_bytes>                          (persis content_length byte)
//   - proxy_spec: "direct"  atau  "socks5h://user:pass@127.0.0.1:40001"
//     (bisa juga "socks5h://127.0.0.1:40001" tanpa auth).
//
//   RESPONSE (Bun -> gateway):
//     STATUS <http_status> <retry_after_or_0>\n
//     <Resp-Header>: <value>\n
//     \n
//     <body_bytes>                          (di-stream; aman untuk SSE)
//
//   ERROR (Bun -> gateway, bila transport gagal / proxy tak didukung):
//     ERROR <reason_oneline>\n
//     (gateway menangkap ini dan jatuh ke jalur httpx — jalur WARP itu
//      tetap berfungsi, hanya tanpa fingerprint TLS Bun. Bun = OPT-IN.)
//
// Catatan:
// - Streaming SSE didukung: body di-pipe byte-per-byte, tidak di-buffer.
// - redirect = "manual" (status 30x diteruskan apa adanya ke gateway).
// - Header hop-by-hop dibuang kedua arah.
// - Proxy pakai undici agent bawaan Bun (SocksAgent / ProxyAgent). Bila
//   runtime Bun versi lama tidak punya, sidecar tetap start tapi request
//   proxy dijawab ERROR -> gateway otomatis httpx. Tidak ada crash.

import { serve } from "bun";

const PORT = Number(process.env.BUN_PROXY_PORT || 39001);
const HOST = process.env.BUN_PROXY_HOST || "127.0.0.1";
// Idle timeout socket (detik). Stream SSE panjang tidak mati selama ada byte.
const IDLE_S = Number(process.env.BUN_PROXY_IDLE_S || 300);

// Header hop-by-hop + relay/proxy-specific yang TIDAK diteruskan.
const STRIP = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailers",
  "transfer-encoding",
  "upgrade",
  "x-relay-target",
  "x-relay-path",
]);

// --------------------------------------------------------------------------
// Undici proxy agents yang tersedia di runtime Bun ini.
// Bun meng-expose globalThis.{ProxyAgent,SocksAgent,SocksEndpointAgent}
// (undici). Absen di versi lama -> fallback ERROR (gateway httpx).
// --------------------------------------------------------------------------
const hasSocks = !!(globalThis.SocksAgent || globalThis.SocksEndpointAgent);
const hasHttpProxy = !!(globalThis.ProxyAgent || globalThis.HttpsProxyAgent);

function buildDispatcher(proxySpec) {
  if (!proxySpec || proxySpec === "direct") return undefined;
  try {
    const u = new URL(proxySpec);
    const user = u.username || undefined;
    const pass = u.password || undefined;
    const port = u.port ? Number(u.port) : 1080;
    if (u.protocol === "socks5h:") {
      // DNS via proxy (diperlukan untuk WARP: nama host harus resolve di
      // dalam terowongan, bukan di DNS server lokal).
      const Agent = globalThis.SocksAgent || globalThis.SocksEndpointAgent;
      if (!Agent) return null;
      return new Agent({ origin: `${u.hostname}:${port}`, username: user, password: pass });
    }
    if (u.protocol === "socks5:") {
      const Agent = globalThis.SocksEndpointAgent || globalThis.SocksAgent;
      if (!Agent) return null;
      return new Agent({ origin: `${u.hostname}:${port}`, username: user, password: pass });
    }
    if (u.protocol === "http:" || u.protocol === "https:") {
      const Agent = globalThis.ProxyAgent;
      if (!Agent) return null;
      const auth = user ? `${user}${pass ? `:${pass}` : ""}` : undefined;
      return new Agent({ uri: proxySpec, token: auth ? btoa(auth) : undefined });
    }
    return undefined; // skema tak dikenal -> direct
  } catch (_) {
    return null;
  }
}

// --------------------------------------------------------------------------
// State machine per socket.
// --------------------------------------------------------------------------
function newSocketState() {
  return {
    buf: Buffer.alloc(0),
    phase: "request-line", // "request-line" | "body"
    raw: [],               // baris mentah (request-line + headers)
    method: "",
    url: "",
    clen: 0,
    proxy: "direct",
    headers: {},
    body: Buffer.alloc(0),
    done: false,
  };
}

function parseRequestLine(line) {
  // <METHOD> <url> <clen> <proxy_spec>
  const parts = line.split(" ");
  return {
    method: parts[0] || "",
    url: parts[1] || "",
    clen: parts[2] ? parseInt(parts[2], 10) || 0 : 0,
    proxy: parts[3] || "direct",
  };
}

async function handleRequest(socket, st) {
  if (!st.url) {
    socket.write(Buffer.from("ERROR missing-target-url\n"));
    socket.end();
    return;
  }

  // Proxy yang tak didukung runtime ini + target https -> tidak bisa pipe
  // TLS Bun lewat raw socket. Jawab ERROR; gateway jatuh ke httpx.
  const dispatch = buildDispatcher(st.proxy);
  if (st.proxy && st.proxy !== "direct" && dispatch === null) {
    if (st.url.startsWith("https://")) {
      socket.write(Buffer.from("ERROR proxy-not-supported-in-bun:" + st.proxy.split("@").pop() + "\n"));
      socket.end();
      return;
    }
    // Non-https + proxy tak didukung: tetap coba direct (best effort).
  }

  const opts = {
    method: st.method || "POST",
    headers: st.headers,
    redirect: "manual",
  };
  if (st.body && st.body.length > 0) opts.body = st.body;
  if (dispatch) opts.dispatcher = dispatch;

  try {
    const res = await fetch(st.url, opts);
    const retry = res.headers.get("retry-after") || "0";
    let hdrs = "";
    res.headers.forEach((v, k) => {
      if (!STRIP.has(k.toLowerCase())) hdrs += `${k}: ${v}\n`;
    });
    socket.write(Buffer.from(`STATUS ${res.status} ${retry}\n${hdrs}\n`));
    if (res.body) {
      try {
        for await (const chunk of res.body) socket.write(chunk);
      } catch (_) {
        // klien sudah close; abaikan.
      }
    }
  } catch (e) {
    socket.write(Buffer.from("ERROR " + ((e && e.message) || String(e)).replace(/\n/g, " ") + "\n"));
  }
  socket.end();
}

serve({
  port: PORT,
  hostname: HOST,
  idleTimeout: IDLE_S,
  socket: {
    data(socket, data) {
      let st = socket._st;
      if (!st) st = socket._st = newSocketState();
      st.buf = Buffer.concat([st.buf, data]);

      while (!st.done) {
        if (st.phase === "request-line") {
          const idx = st.buf.indexOf("\n");
          if (idx === -1) break;
          let line = st.buf.subarray(0, idx).toString("utf8");
          st.buf = st.buf.subarray(idx + 1);
          if (line.endsWith("\r")) line = line.slice(0, -1);
          if (line === "") {
            // Header selesai.
            st.phase = "body";
            // Tangani request-line (raw[0]) + headers (raw[1..]).
            const rl = parseRequestLine(st.raw[0] || "");
            st.method = rl.method;
            st.url = rl.url;
            st.clen = rl.clen;
            st.proxy = rl.proxy;
            for (let i = 1; i < st.raw.length; i++) {
              const hline = st.raw[i];
              if (!hline) continue;
              const c = hline.indexOf(":");
              if (c <= 0) continue;
              const k = hline.slice(0, c).trim().toLowerCase();
              const v = hline.slice(c + 1).trim();
              if (!STRIP.has(k)) st.headers[k] = v;
            }
            if (st.clen === 0) {
              st.body = Buffer.alloc(0);
              st.done = true;
              handleRequest(socket, st); // async
              return;
            }
            break; // tunggu body
          }
          st.raw.push(line);
        } else {
          // body
          if (st.buf.length >= st.clen) {
            st.body = st.buf.subarray(0, st.clen);
            st.buf = st.buf.subarray(st.clen);
            st.done = true;
            handleRequest(socket, st);
          }
          break;
        }
      }
    },
    close() {},
    error() {},
  },
});

console.log(
  `[bun-sidecar] listening ${HOST}:${PORT} | ` +
    `socks=${hasSocks} httpProxy=${hasHttpProxy} idle=${IDLE_S}s`,
);

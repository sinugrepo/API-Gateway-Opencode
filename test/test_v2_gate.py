"""Gate v2 /inference/v1 — validasi kontrak (harus ALL PASSED).

Jalankan dari repo root:  python test/test_v2_gate.py
Keluar 0 bila semua passed, 1 bila ada gagal.

Kontrak diverifikasi LIVE terhadap opencode.ai (bisect header/body
2026-10-14) — lihat catatan di `app/services/opencode.py`:
  - endpoint /inference/v1/* (bukan /zen/v1/*)
  - UA `opencode/<channel>/<version>/<client>`
  - header sesi: x-opencode-session + x-opencode-session-id (2 mirror)
  - x-opencode-request DILARANG (ada value -> 403)
  - stream:true wajib (chat & responses)
  - tools kanonik `read` + `shell` (deskripsi+schema PERSIS v2.0.26)
  - tool_choice tidak boleh "none"
  - session prefix counter kecil (bukan digest acak)
  - placeholder system note saat tool kanonik disuntik
  - alias tool klien bentrok read/shell -> read_client/shell_client + restore
"""
import json
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS, FAIL = "PASS", "FAIL"
_results = []


def case(name):
    def deco(fn):
        _results.append((name, fn))
        return fn
    return deco


# ---------- A. endpoint ----------

@case("A1 endpoint config menunjuk /inference/v1/*")
def _a1():
    import app.core.config as c
    assert c.OPENCODE_URL == "https://opencode.ai/inference/v1/chat/completions", c.OPENCODE_URL
    assert c.OPENCODE_RESPONSES_URL == "https://opencode.ai/inference/v1/responses", c.OPENCODE_RESPONSES_URL
    # models fetcher sengaja TIDAK dipindah (masih 200 di /zen/v1/models).
    assert c.OPENCODE_MODELS_URL == "https://opencode.ai/zen/v1/models", c.OPENCODE_MODELS_URL


@case("A2 tidak ada lagi referensi default /zen/v1 chat/responses di config")
def _a2():
    import app.core.config as c
    assert "/zen/v1/chat/completions" not in c.OPENCODE_URL
    assert "/zen/v1/responses" not in c.OPENCODE_RESPONSES_URL


# ---------- B. User-Agent ----------

@case("B1 UA default bentuk v2 opencode/latest/<ver>/cli")
def _b1():
    import app.services.opencode as oc
    ua = oc.OPENCODE_USER_AGENT
    parts = ua.split("/")
    assert len(parts) == 4, ua
    assert parts[0] == "opencode" and parts[1] == "latest", ua
    assert parts[3] == "cli", ua
    # versi x.y.z
    assert len(parts[2].split(".")) == 3, ua


@case("B2 UA ikut header CLI (semua jalur)")
def _b2():
    import app.services.opencode as oc
    h = oc._opencode_cli_headers()
    assert h["User-Agent"] == oc.OPENCODE_USER_AGENT, h
    assert oc._resolve_opencode_headers(None)["User-Agent"] == oc.OPENCODE_USER_AGENT


# ---------- C. header sesi ----------

@case("C1 dua mirror sesi wajib, x-opencode-request dilarang")
def _c1():
    import app.services.opencode as oc
    h = oc._opencode_cli_headers()
    assert h["x-opencode-session"] == h["x-opencode-session-id"], h
    assert h["x-session-affinity"] == h["x-opencode-session"], h
    assert h["x-session-id"] == h["x-opencode-session"], h
    assert "x-opencode-request" not in h, h


@case("C2 _fresh_request_headers membuang x-opencode-request dari base")
def _c2():
    from app.services.opencode import _fresh_request_headers
    base = {"x-opencode-session": "ses_" + "a" * 26,
            "x-opencode-request": "msg_" + "b" * 26,
            "User-Agent": "x"}
    out = _fresh_request_headers(base)
    assert "x-opencode-request" not in out, out
    assert out["x-opencode-session"] == base["x-opencode-session"]
    # base tak dimutasi
    assert "x-opencode-request" in base


@case("C3 _fresh_identity_headers: sesi baru + 2 mirror, request dibuang")
def _c3():
    from app.services.opencode import _fresh_identity_headers
    base = {"x-opencode-session": "ses_" + "a" * 26,
            "x-opencode-session-id": "ses_" + "a" * 26,
            "x-opencode-request": "msg_" + "b" * 26}
    out = _fresh_identity_headers(base)
    assert "x-opencode-request" not in out, out
    assert out["x-opencode-session"] != base["x-opencode-session"]
    assert out["x-opencode-session-id"] == out["x-opencode-session"]
    assert out["x-session-affinity"] == out["x-opencode-session"]


@case("C4 _fresh_retry_targets: mirror sinkron + request dibuang")
def _c4():
    from app.services.opencode import _fresh_retry_targets
    targets = [("https://up/1", {"x-opencode-session": "ses_" + "a" * 26,
                                 "x-opencode-request": "msg_" + "b" * 26})]
    new_targets, session, _key = _fresh_retry_targets(targets, {"prompt_cache_key": "k"})
    url, headers = new_targets[0]
    assert "x-opencode-request" not in headers, headers
    assert headers["x-opencode-session"] == session
    assert headers["x-opencode-session-id"] == session


@case("C5 sesi klien invalid diganti sesi timestamp-valid proxy")
def _c5():
    from app.services.opencode import _resolve_opencode_headers, _is_valid_opencode_id
    h = _resolve_opencode_headers({"x-opencode-session": "ses_NOTVALID!!"})
    assert _is_valid_opencode_id(h["x-opencode-session"], "ses_"), h
    # sesi klien valid dihormati
    good = "ses_" + "0" * 12 + "a" * 14
    h2 = _resolve_opencode_headers({"x-opencode-session": good})
    assert h2["x-opencode-session"] == good, h2


@case("C6 x-opencode-request dari klien TIDAK diteruskan")
def _c6():
    from app.services.opencode import _resolve_opencode_headers
    h = _resolve_opencode_headers({"x-opencode-request": "msg_" + "b" * 26})
    assert "x-opencode-request" not in h, h


# ---------- D. tools kanonik ----------

@case("D1 spec kanonik: read(path/offset/limit) + shell(command/workdir/timeout/background)")
def _d1():
    from app.services.opencode import _OPENCODE_CANONICAL_TOOL_SPECS as s
    assert set(s) == {"read", "shell"}, set(s)
    read = s["read"]
    assert set(read["properties"]) == {"path", "offset", "limit"}, read
    assert read["required"] == ["path"]
    assert read["properties"]["path"]["description"] == "File or directory to read"
    assert read["properties"]["offset"]["type"] == "integer"
    shell = s["shell"]
    assert set(shell["properties"]) == {"command", "workdir", "timeout", "background"}, shell
    assert shell["required"] == ["command"]
    assert shell["properties"]["command"]["description"] == "Shell command string to execute"
    assert shell["properties"]["background"]["type"] == "boolean"


@case("D2 deskripsi kanonik verbatim (prefix khas v2.0.26)")
def _d2():
    from app.services.opencode import _OPENCODE_CANONICAL_TOOL_SPECS as s
    assert s["read"]["description"].startswith(
        "Read the contents of a file or directory. Supports text files, images, and PDFs."
    )
    assert s["shell"]["description"].startswith(
        "Execute a shell command and return its output."
    )
    assert "Background commands return immediately" in s["shell"]["description"]


@case("D3 bentuk chat = nested, responses = flat")
def _d3():
    from app.services.opencode import _canonical_chat_tool, _canonical_responses_tool
    chat = _canonical_chat_tool("read")
    assert chat["type"] == "function" and "function" in chat
    assert chat["function"]["name"] == "read"
    assert "name" not in chat  # nested: nama di dalam function
    flat = _canonical_responses_tool("read")
    assert flat["type"] == "function" and flat["name"] == "read"
    assert "function" not in flat
    assert flat["parameters"]["required"] == ["path"]


@case("D4 ensure_chat_fingerprint_tools menyuntik read+shell nested")
def _d4():
    from app.services.opencode import ensure_chat_fingerprint_tools
    payload = {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
    ensure_chat_fingerprint_tools(payload)
    names = {t["function"]["name"] for t in payload["tools"]}
    assert names == {"read", "shell"}, names
    read = next(t for t in payload["tools"] if t["function"]["name"] == "read")
    assert read["function"]["parameters"]["required"] == ["path"]
    assert read["function"]["description"].startswith("Read the contents of a file")


@case("D5 ensure_responses_fingerprint_tools menyuntik read+shell flat")
def _d5():
    from app.services.opencode import ensure_responses_fingerprint_tools
    payload = {"model": "m", "input": "hi"}
    ensure_responses_fingerprint_tools(payload)
    names = {t["name"] for t in payload["tools"]}
    assert names == {"read", "shell"}, names
    shell = next(t for t in payload["tools"] if t["name"] == "shell")
    assert shell["parameters"]["required"] == ["command"]
    assert "function" not in shell


# ---------- E. placeholder note ----------

@case("E1 note disisipkan saat tool kanonik di-append (chat)")
def _e1():
    from app.services.opencode import ensure_chat_fingerprint_tools, _OPENCODE_PLACEHOLDER_TOOLS_NOTE
    payload = {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
    ensure_chat_fingerprint_tools(payload)
    assert payload["messages"][0]["role"] == "system", payload["messages"]
    assert payload["messages"][0]["content"] == _OPENCODE_PLACEHOLDER_TOOLS_NOTE
    # pesan asli tetap ada
    assert payload["messages"][1] == {"role": "user", "content": "hi"}


@case("E2 redaksi note: pakai 'placeholders', bukan 'not available'")
def _e2():
    from app.services.opencode import _OPENCODE_PLACEHOLDER_TOOLS_NOTE as note
    assert "placeholder" in note.lower()
    assert "not available" not in note.lower()
    assert "other tools you have been given" in note.lower()


@case("E3 note idempoten (tidak dobel)")
def _e3():
    from app.services.opencode import ensure_chat_fingerprint_tools, _OPENCODE_PLACEHOLDER_TOOLS_NOTE
    payload = {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
    ensure_chat_fingerprint_tools(payload)
    ensure_chat_fingerprint_tools(payload)
    systems = [m for m in payload["messages"] if m.get("role") == "system"
               and m.get("content") == _OPENCODE_PLACEHOLDER_TOOLS_NOTE]
    assert len(systems) == 1, payload["messages"]


@case("E4 note responses: input string dibungkus, list diprepend")
def _e4():
    from app.services.opencode import ensure_responses_fingerprint_tools, _OPENCODE_PLACEHOLDER_TOOLS_NOTE
    p1 = {"model": "m", "input": "halo"}
    ensure_responses_fingerprint_tools(p1)
    assert isinstance(p1["input"], list) and p1["input"][0]["role"] == "system"
    assert p1["input"][0]["content"] == _OPENCODE_PLACEHOLDER_TOOLS_NOTE
    p2 = {"model": "m", "input": [{"role": "user", "content": "halo"}]}
    ensure_responses_fingerprint_tools(p2)
    assert p2["input"][0]["role"] == "system"
    assert p2["input"][1]["role"] == "user"


# ---------- F. alias konflik + restore ----------

@case("F1 tool klien bernama persis 'read' -> read_client (tools + history)")
def _f1():
    from app.services.opencode import ensure_chat_fingerprint_tools
    payload = {
        "model": "m",
        "messages": [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": "c1", "type": "function",
                 "function": {"name": "read", "arguments": "{}"}},
            ]},
        ],
        "tools": [{"type": "function", "function": {
            "name": "read", "description": "klien punya read sendiri",
            "parameters": {"type": "object", "properties": {"x": {"type": "string"}}}}},
        ],
    }
    ensure_chat_fingerprint_tools(payload)
    names = {t["function"]["name"] for t in payload["tools"]}
    assert names == {"read", "shell", "read_client"}, names
    # history assistant tool_call ikut di-rename (pesan system note di depan)
    assistant = next(m for m in payload["messages"] if m.get("tool_calls"))
    call_name = assistant["tool_calls"][0]["function"]["name"]
    assert call_name == "read_client", assistant
    # definisi kanonik read yang dipakai (bukan definisi klien)
    canon = next(t for t in payload["tools"] if t["function"]["name"] == "read")
    assert "path" in canon["function"]["parameters"]["properties"]


@case("F2 case-variant (Read/Bash) TIDAK di-alias")
def _f2():
    from app.services.opencode import ensure_chat_fingerprint_tools
    payload = {
        "model": "m",
        "messages": [{"role": "user", "content": "hi"}],
        "tools": [
            {"type": "function", "function": {"name": "Read", "parameters": {"type": "object", "properties": {}}}},
            {"type": "function", "function": {"name": "Bash", "parameters": {"type": "object", "properties": {}}}},
        ],
    }
    ensure_chat_fingerprint_tools(payload)
    names = {t["function"]["name"] for t in payload["tools"]}
    assert "Read" in names and "Bash" in names, names
    assert "read" in names and "shell" in names, names
    assert "read_client" not in names, names


@case("F3 responses: alias di tools + item function_call input")
def _f3():
    from app.services.opencode import ensure_responses_fingerprint_tools
    payload = {
        "model": "m",
        "input": [
            {"type": "function_call", "call_id": "c1", "name": "shell", "arguments": "{}"},
        ],
        "tools": [{"type": "function", "name": "shell", "parameters": {"type": "object", "properties": {}}}],
    }
    ensure_responses_fingerprint_tools(payload)
    names = {t["name"] for t in payload["tools"]}
    assert names == {"read", "shell", "shell_client"}, names
    call_items = [i for i in payload["input"] if i.get("type") == "function_call"]
    assert call_items[0]["name"] == "shell_client", call_items
    assert call_items[0]["call_id"] == "c1"


@case("F4 restore_client_tool_name")
def _f4():
    from app.services.opencode import restore_client_tool_name as r
    assert r("read_client") == "read"
    assert r("shell_client") == "shell"
    assert r("Read") == "Read"
    assert r("Bash") == "Bash"
    assert r(None) is None
    assert r("read_client_extra") == "read_client_extra"


@case("F5 restore pada objek respons (output + item)")
def _f5():
    from app.services.opencode import restore_aliased_names_in_response as r
    obj = {"output": [{"type": "function_call", "name": "read_client", "call_id": "c"}],
           "item": {"type": "function_call", "name": "shell_client"}}
    r(obj)
    assert obj["output"][0]["name"] == "read"
    assert obj["item"]["name"] == "shell"


# ---------- G. tool_choice + counter ----------

@case("G1 coerce_tool_choice_auto: 'none' -> 'auto' bila tools ada")
def _g1():
    from app.services.opencode import coerce_tool_choice_auto
    p = {"tools": [{"type": "function", "function": {"name": "x"}}], "tool_choice": "none"}
    changed = coerce_tool_choice_auto(p, "test")
    assert changed is True and p["tool_choice"] == "auto", p


@case("G2 ensure_chat_fingerprint_tools tidak menyisakan tool_choice 'none'")
def _g2():
    from app.services.opencode import ensure_chat_fingerprint_tools, coerce_tool_choice_auto
    p = {"model": "m", "messages": [{"role": "user", "content": "hi"}], "tool_choice": "none"}
    ensure_chat_fingerprint_tools(p)
    coerce_tool_choice_auto(p, "test")
    assert p.get("tool_choice") == "auto", p


@case("G3 sesi baru: prefix timestamp-valid, counter kecil (bukan digest acak)")
def _g3():
    import app.services.opencode as oc
    for _ in range(20):
        ses = oc._new_opencode_session_id()
        assert oc._is_plausible_opencode_id(ses, "ses_"), ses
        # decode ms ~ sekarang (counter kecil -> tetap plausibel)
        assert oc._decode_opencode_ms(ses[4:16], descending=True) is not None


@case("G4 _create_id counter default 1 (ascending msg)")
def _g4():
    import app.services.opencode as oc
    mid = oc._new_opencode_request_id()
    assert oc._is_plausible_opencode_id(mid, "msg_"), mid


# ---------- H. integrasi bridge ----------

@case("H1 bridge chat->responses: tools kanonik flat + note + stream")
def _h1():
    from app.services.responses_bridge import build_responses_payload_from_chat
    from app.core.schemas import ChatCompletionRequest
    req = ChatCompletionRequest(model="muse-spark-1.3-contributor-free",
                                messages=[{"role": "user", "content": "halo"}])
    payload = build_responses_payload_from_chat(req)
    names = {t["name"] for t in payload.get("tools", [])}
    assert {"read", "shell"} <= names, names
    assert payload.get("stream") is True
    # note placeholder hadir (input list diawali system)
    items = payload.get("input")
    assert isinstance(items, list)
    assert any(isinstance(i, dict) and i.get("role") == "system" for i in items), items[:2]


@case("H2 wire responses: store=false + tool_choice aman + pairing")
def _h2():
    from app.services.opencode import ensure_responses_wire_fields
    payload = {"model": "muse-spark-1.3-contributor-free", "input": "hi",
               "tools": [], "tool_choice": "none"}
    ensure_responses_wire_fields(payload)
    assert payload["store"] is False
    assert payload["tool_choice"] == "auto", payload.get("tool_choice")
    assert payload["max_output_tokens"] == 65536


def main() -> int:
    print(f"menjalankan {len(_results)} case gate v2...")
    print("=" * 70)
    passed = failed = 0
    for name, fn in _results:
        try:
            fn()
            passed += 1
            print(f"[{PASS}] {name}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"[{FAIL}] {name}: {exc}")
            traceback.print_exc()
    print("=" * 70)
    print(f"TOTAL: {passed} passed, {failed} failed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

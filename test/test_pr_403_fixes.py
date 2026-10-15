"""Validasi fix error 403/400 OpenCode (harus ALL PASSED).

Jalankan dari repo root:  python test/test_pr_403_fixes.py
Keluar dengan kode 0 bila semua passed, 1 bila ada yang gagal.

Latar (port dari PR 9router, root cause identik di gateway ini; gate v2
diverifikasi live 2026-10-14 terhadap /inference/v1/*):
  1. User-Agent WAJIB bentuk `opencode/<channel>/<version>/<client>`
     (gaya lama `opencode/1.18.x` -> 403 FreeTierError).
  2. Prefix timestamp ID `ses_`/`msg_` acak -> 403 (di repo ini sudah
     timestamp-valid; diuji regresi).
  3. Nama tool MCP ber-`:`/`.` (mcp:fs:read) -> 400 invalid_request_error
     (wajib ^[a-zA-Z0-9_-]+$).
  4. Item function_call/function_call_output yatim (/compact, interrupt)
     -> 400 "No tool output found for function call ...".
  5. max_output_tokens <16 atau input+output >1.048.576 -> 400.
  6. Model non-Anthropic halusinasi alias parameter tool Claude Code
     (cmd/path/query) -> InputValidationError di Claude Code CLI/Desktop.

Cakupan:
  A. User-Agent bentuk v2 (opencode/latest/<ver>/cli)
  B. ID timestamp-valid (ses_ descending, msg_ ascending)
  C. Sanitasi nama tool Responses
  D. Pairing function_call/function_call_output yatim
  E. Clamp max_output_tokens [16, 65536] + guard 1M
  F. Normalisasi alias parameter tool Claude Code
  G. Integrasi bridge (responses_to_chat, chat_messages_to_responses_input,
     collect_chat_completion)
  (Gate lengkap v2 /inference — endpoint/header/tool kanonik/placeholder note —
   diuji terpisah di test/test_v2_gate.py.)
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


# ---------- A. User-Agent ----------

@case("A1 default UA bentuk v2 opencode/<channel>/<version>/<client>")
def _a1():
    import app.services.opencode as oc
    ua = oc.OPENCODE_USER_AGENT
    assert ua.startswith("opencode/latest/"), ua
    assert ua.endswith("/cli"), ua
    h = oc._opencode_cli_headers()
    assert h["User-Agent"] == ua, h["User-Agent"]


@case("A2 override env OPENCODE_USER_AGENT tetap dihormati")
def _a2():
    import app.services.opencode as oc
    old = oc.OPENCODE_USER_AGENT
    try:
        oc.OPENCODE_USER_AGENT = "opencode/9.9.9 test"
        assert oc._opencode_cli_headers()["User-Agent"] == "opencode/9.9.9 test"
    finally:
        oc.OPENCODE_USER_AGENT = old


# ---------- B. ID timestamp-valid ----------

@case("B1 ses_/msg_ baru timestamp-valid (plausibel, decode ~now)")
def _b1():
    import app.services.opencode as oc
    for _ in range(20):
        ses = oc._new_opencode_session_id()
        msg = oc._new_opencode_request_id()
        assert oc._is_plausible_opencode_id(ses, "ses_"), ses
        assert oc._is_plausible_opencode_id(msg, "msg_"), msg
        assert len(ses) == 30 and len(msg) == 30, (ses, msg)


@case("B2 request ID unik per POST; sesi tidak dipakai ulang sebagai msg")
def _b2():
    import app.services.opencode as oc
    ids = {oc._new_opencode_request_id() for _ in range(50)}
    assert len(ids) == 50


@case("B3 resolver mengisi sesi (2 mirror) & TIDAK mengirim x-opencode-request")
def _b3():
    from app.services.opencode import _resolve_opencode_headers, _is_plausible_opencode_id
    h = _resolve_opencode_headers(None)
    assert _is_plausible_opencode_id(h["x-opencode-session"], "ses_"), h
    assert h["x-opencode-session-id"] == h["x-opencode-session"], h
    assert "x-opencode-request" not in h, h


# ---------- C. Sanitasi nama tool ----------

@case("C1 sanitize_responses_tool_name: ':'/'.'/spasi -> '_'")
def _c1():
    from app.services.opencode import sanitize_responses_tool_name as s
    assert s("mcp:fs:read") == "mcp_fs_read"
    assert s("server.getTime") == "server_getTime"
    assert s("  bash  ") == "bash"
    assert s("ok-name_1") == "ok-name_1"
    assert s("") == "_"
    assert s(None) == "_"


@case("C2 normalize_responses_tools sanitasi nama (flat + function-wrapped)")
def _c2():
    from app.services.opencode import normalize_responses_tools as n
    flat = n([{"name": "mcp:fs:read", "parameters": {"type": "object"}}])
    assert flat and flat[0]["name"] == "mcp_fs_read", flat
    wrapped = n([{
        "type": "function",
        "function": {"name": "server.getTime", "parameters": {"type": "object"}},
    }])
    assert wrapped and wrapped[0]["name"] == "server_getTime", wrapped
    # nama valid tak tersentuh
    ok = n([{"name": "bash", "parameters": {"type": "object"}}])
    assert ok and ok[0]["name"] == "bash", ok


# ---------- D. Pairing orphan ----------

@case("D1 orphan function_call -> dipasangi output sintetis '{}'")
def _d1():
    from app.services.opencode import sanitize_responses_items as s
    items = [
        {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
        {"type": "function_call", "call_id": "call_1", "name": "Bash", "arguments": "{}"},
    ]
    out, n = s(items)
    assert n == 1, n
    assert len(out) == 3, out
    assert out[2] == {"type": "function_call_output", "call_id": "call_1", "output": "{}"}, out[2]


@case("D2 orphan function_call_output (tanpa call) -> dibuang")
def _d2():
    from app.services.opencode import sanitize_responses_items as s
    items = [
        {"type": "function_call_output", "call_id": "call_x", "output": "hasil"},
        {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
    ]
    out, n = s(items)
    assert n == 1 and len(out) == 1 and out[0]["role"] == "user", out


@case("D3 pasangan lengkap tidak disentuh (n=0)")
def _d3():
    from app.services.opencode import sanitize_responses_items as s
    items = [
        {"type": "function_call", "call_id": "c1", "name": "Bash", "arguments": "{}"},
        {"type": "function_call_output", "call_id": "c1", "output": "ok"},
    ]
    out, n = s(items)
    assert n == 0 and out == items, (out, n)


@case("D4 non-list -> apa adanya, n=0; tak melempar")
def _d4():
    from app.services.opencode import sanitize_responses_items as s
    out, n = s(None)
    assert n == 0
    out2, n2 = s("bukan list")
    assert n2 == 0


@case("D5 ensure_responses_wire_fields pairing orphan di payload input")
def _d5():
    from app.services.opencode import ensure_responses_wire_fields
    payload = {
        "model": "muse-spark-1.3-contributor-free",
        "input": [
            {"type": "function_call", "call_id": "call_9", "name": "Bash", "arguments": "{}"},
        ],
    }
    ensure_responses_wire_fields(payload)
    kinds = [i.get("type") for i in payload["input"]]
    assert kinds.count("function_call_output") == 1, payload["input"]
    outs = [i for i in payload["input"] if i.get("type") == "function_call_output"]
    assert outs[0]["call_id"] == "call_9" and outs[0]["output"] == "{}"


# ---------- E. Clamp max_output_tokens ----------

@case("E1 clamp langsung: 1 -> 16; 999999 -> 65536")
def _e1():
    from app.services.opencode import clamp_responses_max_output_tokens as c
    assert c(1) == 16
    assert c(0) == 16
    assert c(-5) == 16
    assert c(999999) == 65536
    assert c(20000) == 20000
    assert c("bukan-angka") == 65536


@case("E2 guard 1M: input raksasa + 65536 -> total <= 1.048.576")
def _e2():
    from app.services.opencode import clamp_responses_max_output_tokens as c
    payload = {"input": [{"role": "user", "content": [{"type": "input_text", "text": "x" * 4_000_000}]}]}
    v = c(65536, payload)
    assert v == 48_576, v  # 1.048.576 - 1.000.000 (estimasi chars//4)


@case("E3 wire fields: <16 dinaikkan, >65536 diturunkan, hilang -> 65536")
def _e3():
    from app.services.opencode import ensure_responses_wire_fields
    p1 = {"model": "muse-spark", "input": [], "max_output_tokens": 1}
    ensure_responses_wire_fields(p1)
    assert p1["max_output_tokens"] == 16, p1
    p2 = {"model": "muse-spark", "input": [], "max_output_tokens": 500000}
    ensure_responses_wire_fields(p2)
    assert p2["max_output_tokens"] == 65536, p2
    p3 = {"model": "muse-spark", "input": []}
    ensure_responses_wire_fields(p3)
    assert p3["max_output_tokens"] == 65536, p3


@case("E4 bridge chat->responses juga memakai clamp")
def _e4():
    from app.services.responses_bridge import build_responses_payload_from_chat
    from app.core.schemas import ChatCompletionRequest
    req = ChatCompletionRequest(
        model="muse-spark-1.3-contributor-free",
        messages=[{"role": "user", "content": "halo"}],
        max_tokens=1,
    )
    payload = build_responses_payload_from_chat(req)
    assert payload["max_output_tokens"] == 16, payload["max_output_tokens"]
    req2 = ChatCompletionRequest(
        model="muse-spark-1.3-contributor-free",
        messages=[{"role": "user", "content": "halo"}],
        max_tokens=999999,
    )
    payload2 = build_responses_payload_from_chat(req2)
    assert payload2["max_output_tokens"] == 65536, payload2["max_output_tokens"]


# ---------- F. normalize_claude_tool_input ----------

@case("F1 Bash: cmd/script/command_line/commandLine/input/code -> command")
def _f1():
    from app.services.opencode import normalize_claude_tool_input as n
    for alias in ("cmd", "script", "command_line", "commandLine", "input", "code"):
        out = n("Bash", {alias: "ls -la"})
        assert out == {"command": "ls -la"}, (alias, out)
    assert n("bash", {"cmd": "pwd"}) == {"command": "pwd"}  # case-insensitive


@case("F2 Read/Edit/Write/MultiEdit: path/file/... -> file_path + old/new/text")
def _f2():
    from app.services.opencode import normalize_claude_tool_input as n
    for alias in ("path", "file", "filepath", "filePath", "filename"):
        assert n("Read", {alias: "/tmp/a.py"}) == {"file_path": "/tmp/a.py"}, alias
    assert n("Edit", {"file_path": "/a", "old_text": "x", "new_text": "y"}) == {
        "file_path": "/a", "old_string": "x", "new_string": "y"}
    assert n("Write", {"filepath": "/a", "text": "isi"}) == {
        "file_path": "/a", "content": "isi"}
    assert n("MultiEdit", {"file": "/a"}) == {"file_path": "/a"}


@case("F3 Glob/Grep: query/search/regex/glob -> pattern; dir/directory -> path")
def _f3():
    from app.services.opencode import normalize_claude_tool_input as n
    for alias in ("query", "search", "regex", "glob"):
        assert n("Grep", {alias: "TODO"}) == {"pattern": "TODO"}, alias
    for alias in ("dir", "directory"):
        assert n("Glob", {alias: "/src"}) == {"path": "/src"}, alias


@case("F4 kanonik yang sudah ada menang; alias dibiarkan")
def _f4():
    from app.services.opencode import normalize_claude_tool_input as n
    assert n("Bash", {"command": "ls", "cmd": "pwd"}) == {"command": "ls", "cmd": "pwd"}
    assert n("Read", {"file_path": "/a"}) == {"file_path": "/a"}


@case("F5 tipe dijaga: string->string, dict->dict, JSON rusak/unknown tool apa adanya")
def _f5():
    from app.services.opencode import normalize_claude_tool_input as n
    out = n("Bash", json.dumps({"cmd": "ls"}))
    assert isinstance(out, str) and json.loads(out) == {"command": "ls"}, out
    out_dict = n("Bash", {"cmd": "ls"})
    assert isinstance(out_dict, dict), out_dict
    assert n("Bash", "{bukan json") == "{bukan json"
    assert n("UnknownTool", {"cmd": "ls"}) == {"cmd": "ls"}
    assert n("Bash", None) is None
    assert n(None, {"cmd": "ls"}) == {"cmd": "ls"}
    # argumen non-dict JSON (mis. list) dikembalikan utuh
    assert n("Bash", "[1, 2]") == "[1, 2]"


# ---------- G. Integrasi ----------

@case("G1 responses output function_call -> tool_calls chat ternormalisasi")
def _g1():
    from app.services.responses_bridge import _responses_output_to_chat
    content, calls = _responses_output_to_chat([
        {"type": "function_call", "call_id": "c1", "name": "Bash",
         "arguments": json.dumps({"cmd": "ls -la"})},
        {"type": "function_call", "call_id": "c2", "name": "Read",
         "arguments": json.dumps({"path": "/tmp/x"})},
    ])
    assert content == "" and len(calls) == 2, (content, calls)
    assert json.loads(calls[0]["function"]["arguments"]) == {"command": "ls -la"}
    assert json.loads(calls[1]["function"]["arguments"]) == {"file_path": "/tmp/x"}


@case("G2 chat -> responses input: tool_call yatim dipasangkan")
def _g2():
    from app.services.responses_bridge import _chat_messages_to_responses_input
    items = _chat_messages_to_responses_input([
        {"role": "user", "content": "jalankan ls"},
        {"role": "assistant", "content": "", "tool_calls": [
            {"id": "call_1", "type": "function",
             "function": {"name": "Bash", "arguments": "{\"cmd\": \"ls\"}"}},
        ]},
        # turn di-interrupt: TIDAK ada role:tool berikutnya
    ])
    types = [i.get("type") for i in items]
    assert types[-2:] == ["function_call", "function_call_output"], types
    assert items[-1]["call_id"] == "call_1" and items[-1]["output"] == "{}", items[-1]


@case("G3 chat_bridge reverse: function_call input -> tool_calls ternormalisasi")
def _g3():
    from app.services.chat_bridge import _responses_item_to_chat_message
    msg = _responses_item_to_chat_message({
        "type": "function_call", "call_id": "c1", "name": "Grep",
        "arguments": json.dumps({"query": "TODO"}),
    })
    call = msg["tool_calls"][0]
    assert json.loads(call["function"]["arguments"]) == {"pattern": "TODO"}, call


@case("G4 collect_chat_completion: tool call SSE -> args ternormalisasi")
def _g4():
    import asyncio

    from app.services.collect import collect_chat_completion

    async def fake_stream():
        yield "data: " + json.dumps({
            "choices": [{"index": 0, "delta": {"tool_calls": [
                {"index": 0, "id": "call_1", "type": "function",
                 "function": {"name": "Bash", "arguments": ""}},
            ]}}],
        }) + "\n\n"
        yield "data: " + json.dumps({
            "choices": [{"index": 0, "delta": {"tool_calls": [
                {"index": 0, "function": {"arguments": json.dumps({"cmd": "ls -la"})}},
            ]}}],
        }) + "\n\n"
        yield "data: [DONE]\n\n"

    content, calls, finish, usage = asyncio.run(
        collect_chat_completion(fake_stream, client_model="m")
    )
    assert len(calls) == 1, calls
    assert json.loads(calls[0]["function"]["arguments"]) == {"command": "ls -la"}, calls


@case("G5 chat_bridge chat_completion_to_responses: function_call ternormalisasi")
def _g5():
    from app.services.chat_bridge import chat_completion_to_responses
    obj = chat_completion_to_responses({
        "choices": [{"message": {"role": "assistant", "content": "", "tool_calls": [
            {"id": "call_1", "type": "function",
             "function": {"name": "Glob", "arguments": json.dumps({"glob": "*.py"})}},
        ]}}],
    }, "muse-spark")
    fc = [o for o in obj["output"] if o.get("type") == "function_call"][0]
    assert json.loads(fc["arguments"]) == {"pattern": "*.py"}, fc


def main() -> int:
    print(f"menjalankan {len(_results)} case...")
    print("=" * 70)
    passed = failed = 0
    for name, fn in _results:
        try:
            fn()
        except Exception:
            failed += 1
            print(f"[{FAIL}] {name}")
            traceback.print_exc(limit=3)
        else:
            passed += 1
            print(f"[{PASS}] {name}")
    print("=" * 70)
    print(f"hasil: {passed} passed, {failed} failed dari {len(_results)} case")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

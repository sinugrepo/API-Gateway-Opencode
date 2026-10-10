"""Offline MCP tests: normalization (inputSchema) + JSON-RPC dispatch + HTTP layer.

Tanpa network/inferensi: tools/call `chat`/`responses` yang butuh upstream
TIDAK dipanggil live — hanya validasi param-nya. Jalankan:
  python test/test_mcp.py   # harus ALL PASSED
"""
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PASSED = 0
FAILED = 0


def check(name, cond, detail=""):
    global PASSED, FAILED
    if cond:
        PASSED += 1
        print(f"[PASS] {name}")
    else:
        FAILED += 1
        print(f"[FAIL] {name} {detail}")


def main():
    import json
    from app.routes import mcp
    from app.services.opencode import normalize_chat_tools, normalize_responses_tools
    from app.services.chat_bridge import _responses_tools_to_chat_tools
    from app.services.responses_bridge import _chat_tools_to_responses_tools
    from app.services.upstream import build_upstream_payload
    from app.core.schemas import ChatCompletionRequest, ChatMessage

    mcp_tools = [{"name": "my_tool", "description": "t",
                  "inputSchema": {"type": "object", "properties": {"q": {"type": "string"}}}}]

    # ── A. normalisasi inputSchema (MCP) -> parameters (OpenAI) ──
    nc = normalize_chat_tools(mcp_tools)
    check("A1 normalize_chat_tools inputSchema",
          nc and nc[0]["function"]["parameters"]["properties"]["q"]["type"] == "string", str(nc)[:200])
    nr = normalize_responses_tools(mcp_tools)
    check("A2 normalize_responses_tools inputSchema",
          nr and nr[0]["parameters"]["properties"]["q"]["type"] == "string", str(nr)[:200])
    c = _responses_tools_to_chat_tools(mcp_tools)
    check("A3 reverse-bridge terima inputSchema",
          c and c[0]["function"]["parameters"]["properties"]["q"]["type"] == "string", str(c)[:200])
    chat_tool = [{"type": "function", "function": {
        "name": "x", "description": "d",
        "inputSchema": {"type": "object", "properties": {}}}, "strict": True}]
    r = _chat_tools_to_responses_tools(chat_tool)
    check("A4 bridge terima inputSchema + strict",
          r and r[0]["name"] == "x" and r[0]["parameters"] == {"type": "object", "properties": {}}
          and r[0].get("strict") is True, str(r)[:200])

    req = ChatCompletionRequest(model="m1", messages=[ChatMessage(role="user", content="hi")], tools=mcp_tools)
    p = build_upstream_payload(req)
    names = {t.get("function", {}).get("name") for t in p.get("tools", [])}
    check("A5 upstream pertahankan MCP tool + fingerprint + auto",
          "my_tool" in names and {"bash", "glob", "grep", "read"} <= names
          and p.get("tool_choice") == "auto", str(sorted(names)))

    # ── B. JSON-RPC dispatch ──
    async def _dispatch():
        out = {}
        out["init"] = await mcp._dispatch_one(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize",
             "params": {"protocolVersion": "2025-06-18"}})
        out["ping"] = await mcp._dispatch_one({"jsonrpc": "2.0", "id": 2, "method": "ping", "params": {}})
        out["list"] = await mcp._dispatch_one({"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {}})
        out["unknown_tool"] = await mcp._dispatch_one(
            {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "nope", "arguments": {}}})
        out["chat_noparams"] = await mcp._dispatch_one(
            {"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "chat", "arguments": {}}})
        out["resp_noparams"] = await mcp._dispatch_one(
            {"jsonrpc": "2.0", "id": 6, "method": "tools/call",
             "params": {"name": "responses", "arguments": {"model": "m"}}})
        out["bogus"] = await mcp._dispatch_one({"jsonrpc": "2.0", "id": 7, "method": "bogus", "params": {}})
        out["notif"] = await mcp._dispatch_one({"jsonrpc": "2.0", "method": "notifications/initialized"})
        out["prompts"] = await mcp._dispatch_one({"jsonrpc": "2.0", "id": 8, "method": "prompts/list", "params": {}})
        out["resources"] = await mcp._dispatch_one({"jsonrpc": "2.0", "id": 9, "method": "resources/list", "params": {}})
        return out

    out = asyncio.run(_dispatch())
    check("B1 initialize negosiasi versi", out["init"]["result"]["protocolVersion"] == "2025-06-18", str(out["init"])[:200])
    check("B2 ping -> {}", out["ping"]["result"] == {}, str(out["ping"])[:200])
    tools = out["list"]["result"]["tools"]
    check("B3 tools/list 4 tools + inputSchema",
          {t["name"] for t in tools} == {"chat", "responses", "list_models", "gateway_props"}
          and all("inputSchema" in t for t in tools), str([t["name"] for t in tools]))
    check("B4 unknown tool -> -32602", out["unknown_tool"]["error"]["code"] == -32602)
    check("B5 chat tanpa model -> -32602", out["chat_noparams"]["error"]["code"] == -32602)
    check("B6 responses tanpa input -> -32602", out["resp_noparams"]["error"]["code"] == -32602)
    check("B7 method asing -> -32601", out["bogus"]["error"]["code"] == -32601)
    check("B8 notifikasi -> None", out["notif"] is None)
    check("B9 prompts/resources kosong",
          out["prompts"]["result"] == {"prompts": []} and out["resources"]["result"] == {"resources": []})

    # ── C. HTTP layer (TestClient in-process, tanpa network) ──
    from fastapi.testclient import TestClient
    from app import create_app
    from app.core.config import GATEWAY_API_KEYS
    headers = {"Authorization": "Bearer " + GATEWAY_API_KEYS[0]} if GATEWAY_API_KEYS else {}
    client = TestClient(create_app())
    resp = client.post("/mcp", headers=headers,
                       json={"jsonrpc": "2.0", "id": 1, "method": "initialize",
                             "params": {"protocolVersion": "2025-06-18"}})
    check("C1 POST /mcp initialize 200", resp.status_code == 200
          and resp.json()["result"]["protocolVersion"] == "2025-06-18", str(resp.text)[:200])
    resp = client.post("/mcp", headers=headers,
                       json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    check("C2 POST /mcp tools/list", resp.status_code == 200
          and {t["name"] for t in resp.json()["result"]["tools"]}
          == {"chat", "responses", "list_models", "gateway_props"}, str(resp.text)[:200])
    resp = client.post("/mcp", headers=headers, json={"jsonrpc": "2.0", "method": "notifications/initialized"})
    check("C3 notifikasi HTTP -> 202", resp.status_code == 202, str(resp.status_code))
    resp = client.get("/mcp", headers=headers)
    check("C4 GET /mcp discovery", resp.status_code == 200 and "tools" in resp.json(), str(resp.text)[:200])
    resp = client.post("/messages", headers=headers,
                       json={"jsonrpc": "2.0", "id": 9, "method": "ping", "params": {}})
    check("C5 POST /messages legacy ping", resp.status_code == 200 and resp.json()["result"] == {},
          str(resp.text)[:200])
    resp = client.get("/v1/props", headers=headers)
    caps = {c["name"] for c in resp.json().get("capabilities", [])}
    check("C6 props iklankan mcp_server + passthrough",
          {"mcp_server", "mcp_tool_passthrough"} <= caps, str(sorted(caps)))

    # ── D. sanitizer skema rekursif + 400 non-retryable (live 400 Console) ──
    import copy as _copy
    from app.services.opencode import (
        _is_non_retryable_400,
        ensure_responses_wire_fields,
        sanitize_tools_for_upstream,
    )
    rec_schema = {"type": "object",
                  "properties": {"node": {"$ref": "#/$defs/Node"}},
                  "$defs": {"Node": {"type": "object",
                                      "properties": {"child": {"$ref": "#/$defs/Node"},
                                                     "name": {"type": "string"}}}}}
    rec_tools = [{"type": "function", "name": "fs", "description": "fs",
                  "parameters": _copy.deepcopy(rec_schema)}]
    cleaned, n_fixed = sanitize_tools_for_upstream(rec_tools)
    child = cleaned[0]["parameters"]["$defs"]["Node"]["properties"]["child"]
    check("D1 siklus $ref diputus (placeholder object)",
          n_fixed == 1 and child.get("type") == "object" and "$ref" not in child, str(child)[:200])
    check("D2 $ref non-siklik + properti lain utuh",
          cleaned[0]["parameters"]["properties"]["node"] == {"$ref": "#/$defs/Node"}
          and cleaned[0]["parameters"]["$defs"]["Node"]["properties"]["name"] == {"type": "string"})
    mut_schema = {"type": "object", "$defs": {
        "A": {"type": "object", "properties": {"b": {"$ref": "#/$defs/B"}}},
        "B": {"type": "object", "properties": {"a": {"$ref": "#/$defs/A"}}}}}
    mut_tools = [{"name": "m", "inputSchema": _copy.deepcopy(mut_schema)}]
    cleaned_m, n_m = sanitize_tools_for_upstream(mut_tools)
    b_ref = cleaned_m[0]["inputSchema"]["$defs"]["A"]["properties"]["b"]
    check("D3 rekursi mutual A<->B diputus (back-edge placeholder)",
          n_m == 1 and b_ref.get("type") == "object" and "$ref" not in b_ref, str(b_ref)[:200])
    plain = [{"type": "function", "function": {"name": "ok", "description": "d",
             "parameters": {"type": "object", "properties": {"q": {"type": "string"}}}}}]
    snapshot = _copy.deepcopy(plain)
    _, n_plain = sanitize_tools_for_upstream(plain)
    check("D4 skema normal tak tersentuh (n_fixed=0)",
          n_plain == 0 and plain == snapshot, str(plain)[:200])
    mcp_rec = [{"name": "mcp_fs", "description": "d", "inputSchema": _copy.deepcopy(rec_schema)}]
    req_rec = ChatCompletionRequest(model="m1", messages=[ChatMessage(role="user", content="hi")],
                                    tools=mcp_rec)
    p_rec = build_upstream_payload(req_rec)
    fs_tool = next(t for t in p_rec["tools"] if t.get("function", {}).get("name") == "mcp_fs")
    fs_child = fs_tool["function"]["parameters"]["$defs"]["Node"]["properties"]["child"]
    check("D5 upstream chat: MCP rekursif dibersihkan + fingerprint",
          "$ref" not in fs_child
          and {"bash", "glob", "grep", "read"} <= {t.get("function", {}).get("name") for t in p_rec["tools"]},
          str(fs_child)[:200])
    from app.services.responses_bridge import build_responses_payload_from_chat
    bp = build_responses_payload_from_chat(req_rec)
    bp_fs = next(t for t in bp["tools"] if t.get("name") == "mcp_fs")
    bp_child = bp_fs["parameters"]["$defs"]["Node"]["properties"]["child"]
    check("D6 bridge responses: rekursi dibersihkan", "$ref" not in bp_child, str(bp_child)[:200])
    check("D7 400 rekursif/invalid_request -> non-retryable",
          _is_non_retryable_400("Recursive JSON schemas are not currently supported")
          and _is_non_retryable_400('{"type":"invalid_request_error","message":"x"}'))
    check("D8 encrypted_content + kosong -> retryable",
          not _is_non_retryable_400("reasoning `encrypted_content` was not issued to this caller")
          and not _is_non_retryable_400("") and not _is_non_retryable_400(None))
    wire = {"model": "muse-spark-1.3-contributor-free", "input": "hi"}
    ensure_responses_wire_fields(wire)
    check("D9 direct responses: prompt_cache_key disintesis",
          isinstance(wire.get("prompt_cache_key"), str) and wire["prompt_cache_key"], str(wire.get("prompt_cache_key")))

    # ── E. paritas MCP vs biasa di direct /v1/responses (§8 opencode-session.md) ──
    # "Biasa" (tanpa MCP tools) lolos, MCP 403 FreeTier: penyebabnya wire
    # direct yang tak lengkap untuk tools MCP — bukan IP.
    from app.services.opencode import ensure_responses_fingerprint_tools
    mcp_mixed = [
        {"type": "function", "name": "mcp__fs__read", "description": "read",
         "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}},
        {"type": "function", "name": "mcp__fs__list", "description": "list",
         "inputSchema": {"type": "object", "properties": {"dir": {"type": "string"}}}},
    ]
    p_mcp = {"model": "m", "input": "hi", "tools": _copy.deepcopy(mcp_mixed)}
    ensure_responses_fingerprint_tools(p_mcp)
    check("E1 tools MCP flat inputSchema -> parameters di direct wire",
          all(isinstance(t.get("parameters"), dict) for t in p_mcp["tools"])
          and {"mcp__fs__read", "mcp__fs__list", "bash", "glob", "grep", "read"}
          <= {t.get("name") for t in p_mcp["tools"]},
          str([(t.get("name"), sorted(t.get("parameters", {}).keys())) for t in p_mcp["tools"]])[:250])
    w2 = {"model": "muse-spark-1.3-contributor-free", "input": "hi",
          "temperature": None, "tools": _copy.deepcopy(mcp_mixed)}
    ensure_responses_wire_fields(w2)
    check("E2 max_output_tokens default + temperature null di-drop",
          w2.get("max_output_tokens") == 65536 and "temperature" not in w2,
          str({k: w2.get(k) for k in ("max_output_tokens", "temperature")}))
    w3 = {"model": "muse-spark-1.3-contributor-free", "input": "hi", "stream": True,
          "reasoning": {"effort": "medium"}, "tools": _copy.deepcopy(mcp_mixed)}
    ensure_responses_wire_fields(w3)
    sec8 = (w3.get("stream") is True and w3.get("store") is False
            and isinstance(w3.get("max_output_tokens"), int)
            and isinstance(w3.get("prompt_cache_key"), str)
            and w3.get("tool_choice") == "auto"
            and w3.get("reasoning", {}).get("effort") == "xhigh"
            and all(isinstance(t.get("parameters"), dict) for t in w3["tools"]))
    check("E3 wire MCP direct §8-lengkap", sec8, str(sorted(w3.keys())))

    # ── F. batas nesting depth provider (400 "maximum nesting depth") ──
    def _jdepth(node, _d=0):
        if isinstance(node, dict):
            return max([_d] + [_jdepth(v, _d + 1) for v in node.values()] + [_d])
        if isinstance(node, list):
            return max([_d] + [_jdepth(v, _d + 1) for v in node] + [_d])
        return _d

    def _deep_schema(levels):
        _s = {"type": "string"}
        for _ in range(levels):
            _s = {"type": "object", "properties": {"x": _s}}
        return _s

    from app.services.opencode import _is_non_retryable_400 as _noretry
    deep_tools = [{"type": "function", "name": "deep", "description": "d",
                   "parameters": _copy.deepcopy(_deep_schema(15))}]
    td, n_d = sanitize_tools_for_upstream(deep_tools)
    check("F1 skema dalam dipadatkan di bawah 10 level",
          n_d == 1 and _jdepth(td[0]["parameters"]) <= 9, str(_jdepth(td[0]["parameters"])))
    shallow = {"type": "object", "properties": {"a": {"type": "string"}},
               "required": ["a"]}
    ts, n_s = sanitize_tools_for_upstream(
        [{"type": "function", "name": "s", "description": "d", "parameters": _copy.deepcopy(shallow)}])
    check("F2 skema dangkal tak tersentuh", n_s == 0 and ts[0]["parameters"] == shallow)
    check("F3 400 nesting depth -> non-retryable (fail-fast)",
          _noretry("JSON schema exceeds the maximum nesting depth of 10 levels"))

    # ── G. additionalProperties:false strict (400 "Invalid JSON schema") ──
    strict_schema = {"type": "object", "additionalProperties": False,
                     "properties": {"analysis_profile": {
                         "type": "object", "properties": {"level": {"type": "string"}}}},
                     "required": ["req"]}
    map_schema = {"type": "object", "properties": {
        "m": {"type": "object", "additionalProperties": {"type": "string"}}}}
    g_tools = [{"type": "function", "name": "a", "description": "a",
                "parameters": _copy.deepcopy(strict_schema)},
               {"type": "function", "name": "b", "description": "b",
                "parameters": _copy.deepcopy(map_schema)}]
    tg, n_g = sanitize_tools_for_upstream(g_tools)
    check("G1 additionalProperties:false dilonggarkan, required utuh",
          n_g >= 1 and "additionalProperties" not in tg[0]["parameters"]
          and tg[0]["parameters"].get("required") == ["req"]
          and tg[0]["parameters"]["properties"]["analysis_profile"]["properties"]
          == {"level": {"type": "string"}}, str(tg[0]["parameters"])[:250])
    check("G2 additionalProperties bentuk-dict ikut di-drop (provider menolak key-nya)",
          "additionalProperties" not in json.dumps(tg[1]["parameters"]),
          str(tg[1]["parameters"])[:200])

    # ── H. error event Kilo-compatible + fail-fast 1 call (fake upstream 400) ──
    import app.services.responses_bridge as _rb
    from app.core import http_client as _http_client
    from fastapi import BackgroundTasks as _BT

    _400_BODY = json.dumps({
        "model": "muse-spark-1.3-contributor-free",
        "error": {"code": None,
                  "message": "Error from provider (Console): Invalid JSON schema: {\"properties\":{}}",
                  "type": "invalid_request_error"},
    }).encode()

    class _Fake400Response:
        status_code = 400
        headers = {}

        def __init__(self, body):
            self._body = body
            self.text = body.decode("utf-8", "replace")

        async def aread(self):
            return self._body

        def aiter_lines(self):
            return self._aiter()

        async def _aiter(self):
            yield ""

    class _Fake400Ctx:
        def __init__(self, resp):
            self._resp = resp

        async def __aenter__(self):
            return self._resp

        async def __aexit__(self, *exc):
            return False

    class _Fake400Client:
        def __init__(self):
            self.calls = []

        def stream(self, method, url, json=None, headers=None, timeout=None):
            self.calls.append(url)
            return _Fake400Ctx(_Fake400Response(_400_BODY))

    _orig_http, _orig_rb = _http_client._get_http, _rb._get_http
    _fake400 = _Fake400Client()
    _http_client._get_http = lambda: _fake400
    _rb._get_http = lambda: _fake400
    try:
        async def _run_gen():
            gen = _rb.responses_stream_generator(
                {"model": "m", "input": "hi", "stream": True, "store": False,
                 "max_output_tokens": 64, "tools": [], "tool_choice": "auto"},
                client_model="m",
                background_tasks=_BT(),
                use_relay=False,
                opencode_headers={"x-opencode-session": "ses_TESTMCPH"},
                # use_proxy=False: tanpa ini pool proxy lokal (proxies.json)
                # ikut diekspansi -> bocor ke network asli di test offline.
                use_proxy=False,
            )
            return [c async for c in gen]
        _chunks = asyncio.run(_run_gen())
    finally:
        _http_client._get_http, _rb._get_http = _orig_http, _orig_rb
    _datas = []
    for _raw in _chunks:
        for _line in _raw.splitlines():
            if _line.startswith("data:"):
                _payload = _line[5:].lstrip()
                if _payload != "[DONE]":
                    try:
                        _datas.append(json.loads(_payload))
                    except (ValueError, TypeError):
                        pass
    check("H1 fail-fast: 400 payload-error hanya 1 upstream call",
          len(_fake400.calls) == 1, str(len(_fake400.calls)))
    _first = _datas[0] if _datas else {}
    check("H2 chunk pertama type:error Kilo-compatible (sequence_number+message, tanpa key error)",
          _first.get("type") == "error"
          and isinstance(_first.get("sequence_number"), int)
          and isinstance(_first.get("message"), str) and "error" not in _first,
          str(_first)[:250])
    check("H3 chunk warisan All responses targets failed tetap ada (kolektor internal)",
          any(isinstance(d.get("error"), dict)
              and d["error"].get("message") == "All responses targets failed" for d in _datas),
          str([d.get("type", d.get("error")) for d in _datas])[:250])

    async def _run_collect():
        from app.services.collect import collect_responses_object

        async def _replay():
            for _c in _chunks:
                yield _c
        return await collect_responses_object(_replay, client_model="m")

    try:
        asyncio.run(_run_collect())
        check("H4 kolektor raise UpstreamError dari chunk warisan", False, "tidak raise")
    except Exception as _exc:  # noqa: BLE001
        from app.core.errors import UpstreamError as _UE
        check("H4 kolektor raise UpstreamError dari chunk warisan (type:error diabaikan)",
              isinstance(_exc, _UE), f"{type(_exc).__name__}: {str(_exc)[:150]}")

    # ── I. black-box recorder FATAL-400 (wire tools + body upstream utuh) ──
    import os as _os
    import tempfile as _tf
    from app.services.opencode import _dump_fatal_400 as _dump
    _dbg_path = _dump("m", {"tools": [{"name": "t", "parameters": {"type": "object"}}]},
                      400, b'{"error": {"message": "Invalid JSON schema"}}')
    try:
        _doc = json.load(open(_dbg_path))
        check("I1 dump berisi wire_tools + upstream_body utuh",
              _doc["wire_tools"] == [{"name": "t", "parameters": {"type": "object"}}]
              and "Invalid JSON schema" in _doc["upstream_body"]
              and _doc["upstream_status"] == 400 and _doc["model"] == "m",
              str(sorted(_doc.keys())))
    finally:
        try:
            _os.remove(_dbg_path)
        except OSError:
            pass
    _dbg2 = _dump(None, {"tools": None}, None, None)
    try:
        _doc2 = json.load(open(_dbg2))
        check("I2 dump tak pernah melempar (input rusak -> degradasi anggun)",
              isinstance(_doc2, dict) and "wire_tools" in _doc2 and "upstream_body" in _doc2,
              str(sorted(_doc2.keys())) if isinstance(_doc2, dict) else repr(_dbg2))
    finally:
        try:
            _os.remove(_dbg2)
        except OSError:
            pass

    print("=" * 70)
    print(f"hasil: {PASSED} passed, {FAILED} failed dari {PASSED + FAILED} case")
    if FAILED:
        print("RESULT: FAILED")
        sys.exit(1)
    print("RESULT: ALL PASSED")


if __name__ == "__main__":
    main()

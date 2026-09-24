"""Validasi koersi tool_choice -> auto (harus ALL PASSED).

Jalankan dari repo root:  python test/test_tool_choice_coerce.py
Keluar dengan kode 0 bila semua passed, 1 bila ada yang gagal.

Latar (live 2026-09-24): Kilo Code mengirim tool_choice named/required/
none ke spark & mimo; provider Console HANYA mendukung "auto" -> 400
invalid_request_error di SEMUA target (relay+direct) -> 502 ke klien.
Ini salah payload (bukan salah route) sehingga rotasi takkan sembuh.
Perbaikan: koersi di semua builder payload upstream (chat, bridge,
responses-wire, reverse bridge via pipeline chat).

Cakupan (semua offline, tanpa network):
  A. unit coerce_tool_choice_auto (7 varian)
  B. build_upstream_payload: required + tools -> auto
  C. build_responses_payload_from_chat: named -> auto; tanpa tools -> drop
  D. ensure_responses_wire_fields: required -> auto
  E. reverse bridge end-to-end: required -> auto di wire chat
"""
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


def _tools():
    return [{"type": "function",
             "function": {"name": "bash", "description": "d",
                          "parameters": {"type": "object", "properties": {}}}}]


# ---------- A. unit ----------

@case("A1 named/required/none + tools -> auto (changed=True)")
def _a1():
    from app.services.opencode import coerce_tool_choice_auto
    for bad in ({"type": "function", "function": {"name": "bash"}},
                {"type": "function", "name": "bash"},
                "required", "none", True, 123):
        p = {"tools": _tools(), "tool_choice": bad}
        assert coerce_tool_choice_auto(p, "t") is True, bad
        assert p["tool_choice"] == "auto", (bad, p)


@case("A2 auto + tools -> tidak berubah (False)")
def _a2():
    from app.services.opencode import coerce_tool_choice_auto
    p = {"tools": _tools(), "tool_choice": "auto"}
    assert coerce_tool_choice_auto(p, "t") is False
    assert p["tool_choice"] == "auto"


@case("A3 tanpa tools -> key di-drop (True); tanpa key -> False")
def _a3():
    from app.services.opencode import coerce_tool_choice_auto
    p = {"tool_choice": "required"}
    assert coerce_tool_choice_auto(p, "t") is True
    assert "tool_choice" not in p
    assert coerce_tool_choice_auto({}, "t") is False
    assert coerce_tool_choice_auto(None, "t") is False
    assert coerce_tool_choice_auto("bukan-dict", "t") is False


# ---------- B/C/D/E. integrasi builder ----------

@case("B1 chat wire: required + tools -> auto")
def _b1():
    from app.core.schemas import ChatCompletionRequest, ChatMessage
    from app.services.upstream import build_upstream_payload
    req = ChatCompletionRequest(
        model="mimo-v2.6-flash-free",
        messages=[ChatMessage(role="user", content="hi")],
        tools=[{"type": "function",
                "function": {"name": "bash", "description": "d",
                             "parameters": {"type": "object", "properties": {}}}}],
        tool_choice="required",
    )
    payload = build_upstream_payload(req)
    assert payload["tool_choice"] == "auto", payload.get("tool_choice")


@case("C1 bridge: named choice -> auto; tanpa tools -> drop")
def _c1():
    from app.core.schemas import ChatCompletionRequest, ChatMessage
    from app.services.responses_bridge import build_responses_payload_from_chat
    tools = [{"type": "function",
              "function": {"name": "bash", "description": "d",
                           "parameters": {"type": "object", "properties": {}}}}]
    req = ChatCompletionRequest(
        model="muse-spark-1.3-contributor-free",
        messages=[ChatMessage(role="user", content="hi")],
        tools=tools,
        tool_choice={"type": "function", "function": {"name": "bash"}},
    )
    payload = build_responses_payload_from_chat(req)
    assert payload["tool_choice"] == "auto", payload.get("tool_choice")
    # Tanpa tools klien pun kuartet fingerprint disuntik -> tetap auto.
    req2 = ChatCompletionRequest(
        model="muse-spark-1.3-contributor-free",
        messages=[ChatMessage(role="user", content="hi")],
        tool_choice="none",
    )
    payload2 = build_responses_payload_from_chat(req2)
    assert payload2["tool_choice"] == "auto", payload2.get("tool_choice")
    assert len(payload2["tools"]) >= 4  # kuartet fingerprint hadir


@case("D1 responses wire: required -> auto")
def _d1():
    from app.services.opencode import ensure_responses_wire_fields
    p = {"model": "m", "tools": [{"type": "function", "name": "bash"}],
         "tool_choice": "required", "stream": True}
    ensure_responses_wire_fields(p)
    assert p["tool_choice"] == "auto", p.get("tool_choice")


@case("E1 reverse bridge: required -> auto di wire chat akhir")
def _e1():
    from app.core.schemas import ChatCompletionRequest
    from app.services.chat_bridge import build_chat_request_from_responses
    from app.services.upstream import build_upstream_payload
    req: ChatCompletionRequest = build_chat_request_from_responses(
        {"model": "mimo-v2.6-flash-free", "input": "hi",
         "tools": [{"type": "function", "name": "bash"}],
         "tool_choice": "required"},
        "mimo-v2.6-flash-free",
    )
    payload = build_upstream_payload(req)
    assert payload["tool_choice"] == "auto", payload.get("tool_choice")


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
            traceback.print_exc(limit=5)
        else:
            passed += 1
            print(f"[{PASS}] {name}")
    print("=" * 70)
    print(f"hasil: {passed} passed, {failed} failed dari {len(_results)} case")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

"""Backward-compatible entry point (monolith split into `app/` package).

Run with:  uvicorn main:app --host 0.0.0.0 --port 8000
"""
from app import app, create_app

__all__ = ["app", "create_app"]


def __getattr__(name: str):
    """Delegate legacy `from main import X` to the new package."""
    import importlib

    for module in (
        "app.core.config",
        "app.core.logging_utils",
        "app.security.monitor_auth",
        "app.core.schemas",
        "app.core.errors",
        "app.core.http_client",
        "app.services.opencode",
        "app.services.models_cache",
        "app.services.relay",
        "app.core.sse",
        "app.services.usage",
        "app.services.upstream",
        "app.services.tools_dsml",
        "app.services.streaming",
        "app.services.responses_bridge",
        "app.security.scan_guard",
        "app.core.error_handlers",
    ):
        try:
            mod = importlib.import_module(module)
        except ImportError:
            continue
        if hasattr(mod, name):
            return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


if __name__ == "__main__":
    import importlib.util
    import os

    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    # Reload hanya untuk dev (watcher boros CPU + double-start di produksi).
    reload = os.getenv("RELOAD", "false").lower() == "true"
    # uvloop di Linux bila terpasang (VPS): throughput concurrent jauh di
    # atas asyncio bawaan; absen di Windows -> jatuh ke asyncio otomatis.
    loop = "uvloop" if importlib.util.find_spec("uvloop") else "asyncio"
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=reload, loop=loop)

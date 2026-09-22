"""OpenAI-compatible Sinug Gateway backend (modular package).

Original monolith `main.py` (dulunya `backend_api.py`, 6045 lines) split into:
- `app/config.py`, `app/logging_utils.py`, `app/monitor_auth.py`, ...
- `app/routes/*` (endpoints), `app/web/templates/*` (website)
"""
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import APP_VERSION, HERMES_COMPAT, MODEL, MONITOR_PASSWORD, USE_RELAY
from app.core.error_handlers import register_error_handlers
from app.core.http_client import _close_http, _get_http
from app.core.logging_utils import _log
from app.security.scan_guard import ScanGuardMiddleware
from app.security.body_limit import BodyLimitMiddleware
from app.services.usage import _get_usage_db
from app.core.config import USAGE_DB_PATH


@asynccontextmanager
async def lifespan(app: FastAPI):
    _get_usage_db()
    _get_http()
    _log("INFO", f"Usage DB ready at {os.path.abspath(USAGE_DB_PATH)}")
    _log("INFO", "Backend API ready at http://0.0.0.0:8000")
    _log("INFO", f"Model: {MODEL}")
    _log("INFO", f"Relay: {'ON' if USE_RELAY else 'OFF'}")
    _log("INFO", f"Hermes compatibility: {'ON' if HERMES_COMPAT else 'OFF'}")
    if MONITOR_PASSWORD == "admin123":
        _log("WARN", "MONITOR_PASSWORD is still default 'admin123'! Set a strong password via env var.")
        enforce = os.getenv("ENFORCE_MONITOR_PASSWORD", "false").lower() == "true"
        if enforce:
            _log("ERROR", "ENFORCE_MONITOR_PASSWORD is true but password is still default. Refusing to start.")
            raise RuntimeError("MONITOR_PASSWORD must be changed when ENFORCE_MONITOR_PASSWORD=true")
    _log("INFO", "Monitor dashboard at /monitor")
    yield
    await _close_http()


def create_app() -> FastAPI:
    application = FastAPI(
        title="OpenAI-compatible Sinug Gateway Backend",
        version=APP_VERSION,
        lifespan=lifespan,
    )
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    application.add_middleware(ScanGuardMiddleware)
    # BodyLimit paling luar (dieksekusi pertama): tolak body raksasa via
    # Content-Length sebelum request menyentuh route/CORS apa pun.
    application.add_middleware(BodyLimitMiddleware)
    register_error_handlers(application)

    from .routes import chat, misc, monitor, responses_api, usage_routes

    application.include_router(misc.router)
    application.include_router(chat.router)
    application.include_router(responses_api.router)
    application.include_router(usage_routes.router)
    application.include_router(monitor.router)
    return application


app = create_app()

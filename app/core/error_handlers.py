"""Error mapping (OpenAI-compatible bounded errors)."""
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi import HTTPException
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_500_INTERNAL_SERVER_ERROR,
    HTTP_502_BAD_GATEWAY,
    HTTP_504_GATEWAY_TIMEOUT,
)

from app.core.errors import TimeoutError_, UpstreamEmptyResponse, UpstreamError

def _error_response(
    status_code: int,
    message: str,
    code: str,
    *,
    error_type: str = "proxy_error",
    headers: Optional[Dict[str, str]] = None,
    **details: Any,
) -> JSONResponse:
    """Return one bounded OpenAI-compatible error object.

    Keeping errors structured and non-verbose prevents exception details from
    being repeated by SDK retry handlers or leaked into streaming clients.
    """
    error: Dict[str, Any] = {
        "message": message,
        "type": error_type,
        "code": code,
    }
    error.update({key: value for key, value in details.items() if value is not None})
    return JSONResponse(
        status_code=status_code,
        content={"error": error},
        headers=headers or {},
    )


async def validation_error_handler(request: Request, exc: RequestValidationError):
    errors = []
    for error in exc.errors():
        location = " -> ".join(str(item) for item in error.get("loc", []))
        message = error.get("msg", "Invalid request")
        errors.append(f"{location}: {message}" if location else message)

    return _error_response(
        HTTP_400_BAD_REQUEST,
        "; ".join(errors) or "Invalid request",
        "INVALID_REQUEST",
        error_type="invalid_request_error",
        validation_errors=errors,
    )


async def http_exception_handler(request: Request, exc: HTTPException):
    detail = exc.detail if isinstance(exc.detail, str) else "Invalid request"
    return _error_response(
        exc.status_code,
        detail,
        f"HTTP_{exc.status_code}",
        error_type="invalid_request_error" if exc.status_code < 500 else "proxy_error",
    )


async def timeout_handler(request: Request, exc: TimeoutError_):
    return _error_response(
        HTTP_504_GATEWAY_TIMEOUT,
        "Upstream server did not respond in time",
        "TIMEOUT",
        error_type="timeout_error",
    )


async def upstream_error_handler(request: Request, exc: UpstreamError):
    headers: Dict[str, str] = {}
    details: Dict[str, Any] = {"upstream_status": exc.upstream_status}
    if exc.retry_after is not None:
        details["retry_after"] = exc.retry_after
        # HTTP Retry-After delta-seconds must be a non-negative integer.
        headers["Retry-After"] = str(max(1, int(exc.retry_after)))
    return _error_response(
        exc.status_code,
        exc.message,
        f"UPSTREAM_{exc.status_code}",
        error_type="rate_limit_error" if exc.status_code == 429 else "upstream_error",
        headers=headers,
        **details,
    )


async def empty_response_handler(request: Request, exc: UpstreamEmptyResponse):
    return _error_response(
        HTTP_502_BAD_GATEWAY,
        "Upstream returned an empty response",
        "EMPTY_RESPONSE",
        error_type="upstream_error",
    )


async def general_error_handler(request: Request, exc: Exception):
    # Do not serialize the raw exception or traceback into every client retry.
    return _error_response(
        HTTP_500_INTERNAL_SERVER_ERROR,
        "An unexpected error occurred",
        "INTERNAL_ERROR",
        error_type="server_error",
    )


def register_error_handlers(app: FastAPI) -> None:
    app.exception_handler(RequestValidationError)(validation_error_handler)
    app.exception_handler(HTTPException)(http_exception_handler)
    from app.core.errors import TimeoutError_, UpstreamEmptyResponse, UpstreamError
    app.exception_handler(TimeoutError_)(timeout_handler)
    app.exception_handler(UpstreamError)(upstream_error_handler)
    app.exception_handler(UpstreamEmptyResponse)(empty_response_handler)
    app.exception_handler(Exception)(general_error_handler)

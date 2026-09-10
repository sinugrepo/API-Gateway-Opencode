"""Proxy exception types (mapped to HTTP in error_handlers)."""
from typing import Optional

from starlette.status import HTTP_502_BAD_GATEWAY


class UpstreamError(Exception):
    def __init__(
        self,
        message: str,
        status_code: int = HTTP_502_BAD_GATEWAY,
        upstream_status: Optional[int] = None,
        retry_after: Optional[float] = None,
    ):
        self.message = message
        self.status_code = status_code
        self.upstream_status = upstream_status
        self.retry_after = retry_after


class TimeoutError_(Exception):
    pass


class UpstreamEmptyResponse(Exception):
    pass

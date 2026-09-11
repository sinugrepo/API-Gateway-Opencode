"""Request body size guard (ASGI middleware, streaming-safe).

Menolak body raksasa dengan 413 SEBELUM menyentuh aplikasi: hanya membaca
header Content-Length, tidak pernah mem-buffer body. Respons streaming
(SSE) tidak terganggu karena jalur respons diteruskan apa adanya.

Tanpa Content-Length (chunked), request diloloskan — menegakkannya butuh
mem-buffer body, yang justru mengalahkan tujuan middleware ini.
"""
from typing import Any, Dict

from app.core.config import MAX_REQUEST_BYTES


class BodyLimitMiddleware:
    """Tolak request dengan Content-Length > MAX_REQUEST_BYTES."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(
        self, scope: Dict[str, Any], receive: Any, send: Any
    ) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        try:
            for raw_key, raw_val in scope.get("headers", []):
                if raw_key.decode("latin-1").lower() == "content-length":
                    if int(raw_val.decode("latin-1").strip()) > MAX_REQUEST_BYTES:
                        await self._reject(send)
                        return
                    break
        except (ValueError, AttributeError, UnicodeDecodeError):
            pass
        await self.app(scope, receive, send)

    @staticmethod
    async def _reject(send: Any) -> None:
        body = (
            b'{"error":{"message":"Request body too large",'
            b'"type":"invalid_request_error","code":"BODY_TOO_LARGE"}}'
        )
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [(b"content-type", b"application/json"),
                            (b"content-length", str(len(body)).encode())],
            }
        )
        await send({"type": "http.response.body", "body": body})

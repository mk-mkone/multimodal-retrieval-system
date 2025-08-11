import time
import uuid

from fastapi import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from app.core.logging_factory import LoggerFactory


class RequestLoggingMiddleware:
    """
    Middleware that wraps incoming HTTP requests to inject a request ID,
    log timing, method, path, and status code, and handle unhandled exceptions
    with appropriate logging.
    """

    def __init__(self, app: ASGIApp):
        """
        Stores the ASGI application reference and initializes the logger.
        """
        self.app = app
        self.logger = LoggerFactory.get_logger("http")

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        """
        ASGI entry point. Intercepts HTTP requests to add a `x-request-id` header
        to responses, logs request metadata, and measures processing duration.
        Logs request details on normal completion and logs errors on exceptions.
        """
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        request = Request(scope, receive=receive)
        request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
        method = request.method
        path = scope.get("path", "-")
        start = time.perf_counter()

        status_code_holder = {"value": 500}  # par défaut si exception

        async def send_wrapper(message):
            """
            Wraps the ASGI send callable to intercept the response start event,
            capture the status code, and append the request ID to the headers.
            """
            if message["type"] == "http.response.start":
                status_code_holder["value"] = message["status"]
                headers = list(message.get("headers", []))
                headers.append((b"x-request-id", request_id.encode("utf-8")))
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as exc:
            duration_ms = int((time.perf_counter() - start) * 1000)
            self.logger.error(
                "unhandled exception",
                extra={
                    "request_id": request_id,
                    "method": method,
                    "path": path,
                    "duration_ms": duration_ms,
                    "status_code": 500,
                    "error": repr(exc),
                },
            )
            raise
        else:
            duration_ms = int((time.perf_counter() - start) * 1000)
            self.logger.info(
                "request handled",
                extra={
                    "request_id": request_id,
                    "method": method,
                    "path": path,
                    "duration_ms": duration_ms,
                    "status_code": status_code_holder["value"],
                },
            )

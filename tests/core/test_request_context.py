import uuid
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.request_context import RequestLoggingMiddleware


def _make_app_with_middleware(mocker):
    """
    Construit une app FastAPI avec le middleware et renvoie (client, mock_logger).
    On patch `LoggerFactory.get_logger` AVANT d'ajouter le middleware.
    """
    mock_logger = mocker.Mock()
    mocker.patch(
        "app.core.request_context.LoggerFactory.get_logger",
        return_value=mock_logger,
    )

    app = FastAPI()
    app.add_middleware(RequestLoggingMiddleware)

    @app.get("/ok")
    def ok():
        return {"ok": True}

    @app.get("/boom")
    def boom():
        raise RuntimeError("boom")

    return TestClient(app, raise_server_exceptions=False), mock_logger


def test_adds_request_id_when_missing(mocker):
    client, mock_logger = _make_app_with_middleware(mocker)

    resp = client.get("/ok")
    assert resp.status_code == 200

    req_id = resp.headers.get("x-request-id")
    assert req_id, "x-request-id header should be present"
    uuid.UUID(req_id)

    assert mock_logger.info.call_count == 1
    args, kwargs = mock_logger.info.call_args
    assert args[0] == "request handled"
    extra = kwargs.get("extra", {})
    assert extra.get("request_id") == req_id
    assert extra.get("method") == "GET"
    assert extra.get("path") == "/ok"
    assert extra.get("status_code") == 200
    assert isinstance(extra.get("duration_ms"), int)
    assert extra["duration_ms"] >= 0


def test_preserves_request_id_when_present(mocker):
    client, mock_logger = _make_app_with_middleware(mocker)

    given_id = "unit-test-id-123"
    resp = client.get("/ok", headers={"x-request-id": given_id})
    assert resp.status_code == 200
    assert resp.headers.get("x-request-id") == given_id

    args, kwargs = mock_logger.info.call_args
    extra = kwargs.get("extra", {})
    assert extra.get("request_id") == given_id
    assert extra.get("status_code") == 200


def test_logs_error_on_exception_and_preserves_http_request_id(mocker):
    client, mock_logger = _make_app_with_middleware(mocker)

    given_id = "err-req-id-42"
    resp = client.get("/boom", headers={"x-request-id": given_id})
    assert resp.status_code == 500

    assert mock_logger.error.call_count == 1
    args, kwargs = mock_logger.error.call_args
    assert args[0] == "unhandled exception"
    extra = kwargs.get("extra", {})

    assert extra.get("request_id") == given_id
    assert extra.get("method") == "GET"
    assert extra.get("path") == "/boom"
    assert extra.get("status_code") == 500
    assert isinstance(extra.get("duration_ms"), int)
    assert extra["duration_ms"] >= 0
    assert "RuntimeError" in extra.get("error", "")

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.logging_factory import JsonFormatter, LoggerFactory


def _reset_logging_and_factory():
    root = logging.getLogger()
    root.handlers.clear()
    LoggerFactory._configured = False


@pytest.fixture(autouse=True)
def reset_logging():
    _reset_logging_and_factory()
    yield
    _reset_logging_and_factory()


def _set_settings(monkeypatch, **kwargs):
    from app.core.config import settings

    for k, v in kwargs.items():
        monkeypatch.setattr(settings, k, v, raising=False)
    return settings


def _capture_stderr(capfd):
    out, err = capfd.readouterr()
    lines = [l for l in err.strip().splitlines() if l.strip()]
    return lines[-1] if lines else ""


def test_json_formatter_includes_extras_and_excludes_std(monkeypatch, capfd):
    _set_settings(monkeypatch, LOG_FORMAT="json", LOG_FILE_ENABLED=False)

    logger = LoggerFactory.get_logger("test.json.extras")
    logger.info("Hello", extra={"source": "unit", "request_id": "abc123"})

    line = _capture_stderr(capfd)
    payload = json.loads(line)

    assert payload["level"] == "INFO"
    assert payload["logger"] == "test.json.extras"
    assert payload["message"] == "Hello"
    assert "module" in payload and "func" in payload and "line" in payload

    assert payload["source"] == "unit"
    assert payload["request_id"] == "abc123"

    assert "exc_info" not in payload
    assert "stack" not in payload


def test_timestamp_is_utc_iso8601_with_Z(monkeypatch, capfd):
    _set_settings(monkeypatch, LOG_FORMAT="json", LOG_FILE_ENABLED=False)

    logger = LoggerFactory.get_logger("test.json.time")
    logger.info("Time?")

    line = _capture_stderr(capfd)
    payload = json.loads(line)

    ts = payload["timestamp"]
    assert ts.endswith("Z")
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    assert dt.tzinfo is not None
    assert dt.tzinfo.utcoffset(dt) == timezone.utc.utcoffset(dt)


def test_exc_info_is_serialized_when_present(monkeypatch, capfd):
    _set_settings(monkeypatch, LOG_FORMAT="json", LOG_FILE_ENABLED=False)

    logger = LoggerFactory.get_logger("test.json.exc")
    try:
        raise ValueError("boom")
    except ValueError:
        logger.exception("Got exception")

    payload = json.loads(_capture_stderr(capfd))
    assert "exc_info" in payload
    assert "ValueError" in payload["exc_info"]
    assert "boom" in payload["exc_info"]


def test_stack_info_is_serialized_when_requested(monkeypatch, capfd):
    _set_settings(monkeypatch, LOG_FORMAT="json", LOG_FILE_ENABLED=False)

    logger = LoggerFactory.get_logger("test.json.stack")
    logger.info("With stack", stack_info=True)

    payload = json.loads(_capture_stderr(capfd))
    assert "stack" in payload
    assert "Stack (most recent call last)" in payload["stack"]


def test_plain_text_formatter_when_not_json(monkeypatch, capfd):
    _set_settings(monkeypatch, LOG_FORMAT="text", LOG_FILE_ENABLED=False)

    logger = LoggerFactory.get_logger("test.text")
    logger.warning("Plain text log")

    line = _capture_stderr(capfd)
    with pytest.raises(json.JSONDecodeError):
        json.loads(line)
    assert "WARNING" in line
    assert "test.text" in line
    assert "Plain text log" in line


def test_rotating_file_handler_creates_log_file(monkeypatch, tmp_path: Path):
    log_file = tmp_path / "logs" / "app.log"
    _set_settings(
        monkeypatch,
        LOG_FORMAT="json",
        LOG_FILE_ENABLED=True,
        LOG_FILE=str(log_file),
    )

    logger = LoggerFactory.get_logger("test.file")
    logger.info("to file")

    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8").strip().splitlines()
    assert content, "log file should not be empty"
    payload = json.loads(content[-1])
    assert payload["logger"] == "test.file"
    assert payload["message"] == "to file"


def test_config_is_idempotent_no_duplicate_handlers(monkeypatch):
    _set_settings(monkeypatch, LOG_FORMAT="json", LOG_FILE_ENABLED=False)

    root = logging.getLogger()

    LoggerFactory.get_logger("a.b.c")
    first_handlers = list(root.handlers)
    first_ids = {id(h) for h in first_handlers}

    assert len(first_handlers) >= 1

    LoggerFactory.get_logger("x.y.z")
    second_ids = {id(h) for h in root.handlers}

    assert (
        second_ids == first_ids
    ), "should not add duplicate handlers on subsequent get_logger()"


def test_uvicorn_logs_propagate(monkeypatch):
    _set_settings(monkeypatch, LOG_FORMAT="json", LOG_FILE_ENABLED=False)

    _ = LoggerFactory.get_logger("init")

    assert logging.getLogger("uvicorn").propagate is True
    assert logging.getLogger("uvicorn.access").propagate is True


def test_jsonformatter_direct_format(record_factory):
    """
    Test bas-niveau sur JsonFormatter.format() avec un LogRecord custom,
    pour valider l’extraction des extras via record.__dict__ (zone sensible).
    """
    formatter = JsonFormatter()

    logger_name = "unit.formatter"
    rec = record_factory(
        name=logger_name,
        level=logging.INFO,
        fn="module.py",
        lno=123,
        msg="Hello %s",
        args=("X",),
        exc_info=None,
    )

    rec.user = "alice"
    rec.request_id = "req-1"
    rec.stack_info = "Stack (most recent call last):\n..."

    text = formatter.format(rec)
    payload = json.loads(text)

    assert payload["logger"] == logger_name
    assert payload["message"] == "Hello X"
    assert payload["user"] == "alice"
    assert payload["request_id"] == "req-1"
    assert payload["stack"]


@pytest.fixture
def record_factory():
    """
    Retourne une fabrique de LogRecord minimal compatible avec logging.makeLogRecord
    mais permettant de fixer fn/lineno/args facilement.
    """

    def _make(**kwargs):
        name = kwargs.get("name", "test")
        level = kwargs.get("level", logging.INFO)
        fn = kwargs.get("fn", __file__)
        lno = kwargs.get("lno", 1)
        msg = kwargs.get("msg", "msg")
        args = kwargs.get("args", ())
        exc_info = kwargs.get("exc_info", None)

        record = logging.LogRecord(
            name, level, fn, lno, msg, args, exc_info, func="func"
        )
        return record

    return _make

import json
import logging
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, Dict

from app.core.config import settings

_STD_KEYS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
    "asctime",
}
"""Set of standard LogRecord attribute names to exclude from extra fields."""


class JsonFormatter(logging.Formatter):
    """Formatter that outputs logs as JSON with support for custom extra fields and UTC timestamps."""

    def formatTime(self, record, datefmt=None):
        """Format the time of the log record as an ISO-8601 UTC timestamp with milliseconds precision."""
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def format(self, record: logging.LogRecord) -> str:
        """Build a JSON structure containing standard log fields and any extra fields,
        including exception and stack information if present."""
        message = record.getMessage()

        extras: Dict[str, Any] = {
            k: v
            for k, v in record.__dict__.items()
            if k not in _STD_KEYS and not k.startswith("_")
        }

        log_record: Dict[str, Any] = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": message,
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
            "pid": record.process,
            "thread": record.threadName,
            **extras,
        }

        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            log_record["stack"] = self.formatStack(record.stack_info)

        return json.dumps(log_record, ensure_ascii=False)


def _make_formatter() -> logging.Formatter:
    """Select and return a JSON or plain-text formatter based on the LOG_FORMAT setting."""
    fmt = (settings.LOG_FORMAT or "json").lower()
    if fmt == "json":
        return JsonFormatter()
    return logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


class LoggerFactory:
    """Factory class responsible for configuring the root logger and providing logger instances."""

    _configured = False

    @classmethod
    def _configure(cls) -> None:
        """Configure the root logger with console and optional rotating file handlers,
        avoiding duplicate handlers on reload."""
        if cls._configured:
            return

        level = (settings.LOG_LEVEL or "INFO").upper()
        formatter = _make_formatter()

        root = logging.getLogger()
        root.setLevel(level)
        root.handlers.clear()

        console = logging.StreamHandler()
        console.setFormatter(formatter)
        root.addHandler(console)

        if bool(settings.LOG_FILE_ENABLED):
            log_file = settings.LOG_FILE or "logs/app.log"
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            file_handler = RotatingFileHandler(
                log_file, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
            )
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)

        for noisy in ("uvicorn", "uvicorn.access"):
            logging.getLogger(noisy).propagate = True

        cls._configured = True

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """Ensure logging is configured and return a logger instance by name."""
        cls._configure()
        return logging.getLogger(name)

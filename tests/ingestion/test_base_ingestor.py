import asyncio
import json
from pathlib import Path

import pandas as pd
import pytest
import requests

from app.ingestion.base import BaseIngestor


class DummyIngestor(BaseIngestor):
    """Concrete ingestor to exercise BaseIngestor plumbing in tests."""

    NAME = "dummy"

    def __init__(self, out_dir: str):
        super().__init__(out_dir=out_dir)
        self.fetch_calls = 0
        self.parse_calls = 0
        self.last_raw = None

    def fetch(self, **kwargs):
        self.fetch_calls += 1
        raw = {"items": [1, 2, 3], "params": kwargs}
        self.last_raw = raw
        return raw

    def parse(self, raw) -> pd.DataFrame:
        self.parse_calls += 1
        items = raw.get("items", []) if isinstance(raw, dict) else []
        return pd.DataFrame({"x": items})


@pytest.fixture
def mock_logger(mocker):
    logger = mocker.Mock()
    logger.info = mocker.Mock()
    logger.warning = mocker.Mock()
    mocker.patch("app.ingestion.base.LoggerFactory.get_logger", return_value=logger)
    return logger


@pytest.fixture
def ing(tmp_path, mock_logger):
    return DummyIngestor(out_dir=str(tmp_path))


class FakeResp:
    def __init__(
        self,
        ok: bool,
        status_code: int = 200,
        payload: dict | None = None,
        exc: Exception | None = None,
    ):
        self.ok = ok
        self.status_code = status_code
        self._payload = payload or {}
        self._exc = exc

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self._exc:
            raise self._exc
        if not self.ok:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def test_save_raw_writes_json_file(ing: DummyIngestor, tmp_path: Path, mock_logger):
    raw = {"a": 1, "b": [1, 2]}
    p = ing.save_raw(raw, suffix="json")
    assert p.exists()
    data = json.loads(p.read_text(encoding="utf-8"))
    assert data == raw
    mock_logger.info.assert_any_call("raw_saved", extra={"path": str(p)})


def test_save_raw_writes_bytes_and_text(ing: DummyIngestor):
    p1 = ing.save_raw(b"hello", suffix="bin")
    assert p1.read_bytes() == b"hello"
    p2 = ing.save_raw("bonjour", suffix="txt")
    assert p2.read_text(encoding="utf-8") == "bonjour"


def test_save_table_csv_writes_file(ing: DummyIngestor, mock_logger):
    df = pd.DataFrame({"a": [1, 2]})
    p = ing._save_table(df, fmt="csv")
    assert p.suffix == ".csv"
    assert p.exists()
    mock_logger.info.assert_any_call("table_saved", extra={"path": str(p), "rows": 2})


def test_run_pipeline_calls_fetch_parse_and_saves_table(mocker, ing: DummyIngestor):
    fake_path = ing.out_dir / "table.csv"
    mocker.patch.object(ing, "_save_table", return_value=fake_path)

    df = ing.run(example=123)
    assert list(df["x"]) == [1, 2, 3]
    assert ing.fetch_calls == 1
    assert ing.parse_calls == 1


def test_run_async_pipeline_uses_threads(mocker, ing: DummyIngestor):
    fake_path = ing.out_dir / "table.csv"
    mocker.patch.object(ing, "_save_table", return_value=fake_path)

    df = asyncio.run(ing.run_async(example=456))
    assert list(df["x"]) == [1, 2, 3]
    assert ing.fetch_calls == 1
    assert ing.parse_calls == 1


def test_http_get_json_retries_then_succeeds(mocker, ing: DummyIngestor):
    seq = [
        FakeResp(False, 500),
        FakeResp(False, 502),
        FakeResp(True, 200, {"ok": True}),
    ]
    get = mocker.patch("app.ingestion.base.requests.get", side_effect=seq)

    out = ing.http_get_json(
        "http://example/api", params={"q": 1}, headers={"X": "a"}, retries=3, backoff=0
    )
    assert out == {"ok": True}
    assert get.call_count == 3


def test_http_get_json_raises_after_retries(mocker, ing: DummyIngestor):
    err = requests.HTTPError("upstream down")
    seq = [FakeResp(False, 500, exc=err)] * 3
    get = mocker.patch("app.ingestion.base.requests.get", side_effect=seq)

    with pytest.raises(requests.HTTPError):
        ing.http_get_json("http://x", retries=3, backoff=0)
    assert get.call_count == 3


def test_http_get_json_async_runs_in_thread(mocker, ing: DummyIngestor):
    mock = mocker.patch.object(ing, "http_get_json", return_value={"ok": 1})
    out = asyncio.run(ing.http_get_json_async("http://x"))
    assert out == {"ok": 1}
    mock.assert_called_once()


def test_brief_variants():
    assert BaseIngestor._brief({"a": 1}) == {"type": "dict", "keys": ["a"]}
    assert BaseIngestor._brief([1, 2, 3]) == {"type": "list", "len": 3}
    assert BaseIngestor._brief("x") == {"type": "str"}

import asyncio
import builtins
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from app.ingestion.timeseries_ingestor import TimeSeriesIngestor


@pytest.fixture
def ts(tmp_path):
    return TimeSeriesIngestor(out_dir=str(tmp_path))


def test_fetch_validates_path_and_returns_str(ts: TimeSeriesIngestor, tmp_path: Path):
    p = tmp_path / "sig.csv"
    p.write_text("t,val\n0,1\n1,2\n", encoding="utf-8")
    out = ts.fetch(str(p), kind="csv")
    assert out == str(p)


def test_fetch_missing_raises(ts: TimeSeriesIngestor, tmp_path: Path):
    p = tmp_path / "missing.csv"
    with pytest.raises(FileNotFoundError):
        ts.fetch(str(p))


def test_fetch_async_validates_path(ts: TimeSeriesIngestor, tmp_path: Path):
    p = tmp_path / "sig.json"
    p.write_text(json.dumps([{"t": 0, "v": 1}, {"t": 1, "v": 2}]), encoding="utf-8")
    out = asyncio.run(ts.fetch_async(str(p), kind="json"))
    assert out == str(p)


def test_parse_csv(ts: TimeSeriesIngestor, tmp_path: Path):
    p = tmp_path / "a.csv"
    p.write_text("t,amp\n0,0.1\n1,0.2\n", encoding="utf-8")
    df = ts.parse(str(p))
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["t", "amp"]
    assert len(df) == 2
    assert df["amp"].tolist() == [0.1, 0.2]


def test_parse_json_lines(ts: TimeSeriesIngestor, tmp_path: Path):
    p = tmp_path / "a.json"
    p.write_text('{"t":0,"v":1}\n{"t":1,"v":2}\n', encoding="utf-8")
    df = ts.parse(str(p))
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) == {"t", "v"}
    assert len(df) == 2
    assert df["v"].tolist() == [1, 2]


def test_parse_netcdf_uses_xarray_if_available(
    monkeypatch, ts: TimeSeriesIngestor, tmp_path: Path
):
    """Mock xarray.open_dataset -> objet avec to_dataframe().reset_index()."""
    p = tmp_path / "s.nc"
    p.write_bytes(b"\x00")

    class DummyDS:
        def to_dataframe(self):
            return pd.DataFrame({"a": [1, 2]}).set_index(pd.Index([10, 11], name="i"))

    dummy_xr = SimpleNamespace(open_dataset=lambda path: DummyDS())
    monkeypatch.setitem(__import__("sys").modules, "xarray", dummy_xr)

    df = ts.parse(str(p))
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["i", "a"]
    assert df["a"].tolist() == [1, 2]


def test_parse_netcdf_raises_if_xarray_missing(
    monkeypatch, ts: TimeSeriesIngestor, tmp_path: Path
):
    p = tmp_path / "s.netcdf"
    p.write_bytes(b"\x00")

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "xarray":
            raise ImportError("no xarray for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(RuntimeError) as exc:
        ts.parse(str(p))
    assert "xarray is required to read NetCDF files" in str(exc.value)


def test_parse_unsupported_extension_raises(ts: TimeSeriesIngestor, tmp_path: Path):
    p = tmp_path / "a.txt"
    p.write_text("whatever", encoding="utf-8")
    with pytest.raises(ValueError) as exc:
        ts.parse(str(p))
    assert "Unsupported file format" in str(exc.value)

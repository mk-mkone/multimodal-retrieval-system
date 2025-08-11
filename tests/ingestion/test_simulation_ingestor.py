import asyncio
from unittest.mock import AsyncMock

import pandas as pd
import pytest
import requests

from app.ingestion.simulation_ingestor import MaterialsProjectIngestor


@pytest.fixture
def mp_api_url(monkeypatch):
    url = "https://api.materialsproject.org/materials/summary"
    monkeypatch.setattr(
        "app.ingestion.simulation_ingestor.settings.MP_API_URL", url, raising=True
    )
    return url


@pytest.fixture
def mp_ingestor(tmp_path):
    return MaterialsProjectIngestor(out_dir=str(tmp_path), api_key="TEST_KEY")


@pytest.fixture
def mp_ingestor_no_key(tmp_path):
    return MaterialsProjectIngestor(out_dir=str(tmp_path), api_key=None)


def test_build_params_defaults():
    params = MaterialsProjectIngestor._build_params()
    assert params["formula"] == "Si"
    assert params["_per_page"] == 25
    assert params["_page"] == 1


def test_build_params_with_extra():
    params = MaterialsProjectIngestor._build_params(
        formula="C", per_page=10, page=3, extra={"fields": "material_id,band_gap"}
    )
    assert params["formula"] == "C"
    assert params["_per_page"] == 10
    assert params["_page"] == 3
    assert params["fields"] == "material_id,band_gap"


def test_fetch_sync_calls_http_with_headers_and_params(mocker, mp_ingestor, mp_api_url):
    fake = {"data": []}
    http_get_json = mocker.patch.object(mp_ingestor, "http_get_json", return_value=fake)

    out = mp_ingestor.fetch(formula="SiO2", per_page=50, page=2)
    assert out == fake

    called_url = http_get_json.call_args.args[0]
    kwargs = http_get_json.call_args.kwargs
    assert called_url == mp_api_url
    assert kwargs["params"]["formula"] == "SiO2"
    assert kwargs["params"]["_per_page"] == 50
    assert kwargs["params"]["_page"] == 2
    assert kwargs["headers"]["X-API-KEY"] == "TEST_KEY"


def test_fetch_propagates_http_error(mocker, mp_ingestor, mp_api_url):
    mocker.patch.object(
        mp_ingestor, "http_get_json", side_effect=requests.HTTPError("upstream")
    )
    with pytest.raises(requests.HTTPError):
        mp_ingestor.fetch()


@pytest.mark.parametrize("formula,per_page,page", [("Al2O3", 5, 4), ("C", 10, 1)])
def test_fetch_async_uses_async_http(
    mocker, mp_ingestor, mp_api_url, formula, per_page, page
):
    http_get_json_async = mocker.patch.object(
        mp_ingestor, "http_get_json_async", new=AsyncMock(return_value={"data": []})
    )
    out = asyncio.run(
        mp_ingestor.fetch_async(formula=formula, per_page=per_page, page=page)
    )
    assert out == {"data": []}

    called_url = http_get_json_async.call_args.args[0]
    kwargs = http_get_json_async.call_args.kwargs
    assert called_url == mp_api_url
    assert kwargs["params"]["formula"] == formula
    assert kwargs["params"]["_per_page"] == per_page
    assert kwargs["params"]["_page"] == page
    assert kwargs["headers"]["X-API-KEY"] == "TEST_KEY"


def test_parse_returns_dataframe_with_expected_columns(mp_ingestor):
    raw = {
        "data": [
            {
                "material_id": "mp-149",
                "formula_pretty": "Si",
                "symmetry": {"symbol": "Fd-3m"},
                "bandstructure": {"band_gap": 1.12},
                "density": 2.33,
            },
            {
                "material_id": "mp-13",
                "formula_pretty": "C",
                "symmetry": {"symbol": "Fd-3m"},
                "bandstructure": {"band_gap": 5.5},
                "density": 3.51,
            },
        ]
    }
    df = mp_ingestor.parse(raw)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == [
        "material_id",
        "formula",
        "spacegroup",
        "band_gap",
        "density",
    ]
    assert len(df) == 2
    assert df.iloc[0]["material_id"] == "mp-149"
    assert df.iloc[1]["formula"] == "C"
    assert pytest.approx(df.iloc[0]["band_gap"], rel=1e-6) == 1.12


def test_parse_empty_returns_empty_dataframe(mp_ingestor):
    df = mp_ingestor.parse({"data": []})
    assert isinstance(df, pd.DataFrame)
    assert df.empty

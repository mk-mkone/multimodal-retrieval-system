import asyncio
from unittest.mock import AsyncMock

import pandas as pd
import pytest
import requests

from app.ingestion.text_ingestor import EuropePMCIngestor


@pytest.fixture
def epmc_url(monkeypatch):
    url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    monkeypatch.setattr(
        "app.ingestion.text_ingestor.settings.EUROPEPMC_API_URL", url, raising=True
    )
    return url


@pytest.fixture
def ing(tmp_path):
    return EuropePMCIngestor(out_dir=str(tmp_path))


def test_build_params_defaults():
    params = EuropePMCIngestor._build_params(query="catalyst")
    assert params["query"] == "catalyst"
    assert params["format"] == "json"
    assert params["page"] == 1
    assert params["pageSize"] == 25
    assert params["resultType"] == "lite"


def test_build_params_overrides():
    params = EuropePMCIngestor._build_params(
        query="battery", page=3, page_size=50, result_type="core"
    )
    assert params["query"] == "battery"
    assert params["page"] == 3
    assert params["pageSize"] == 50
    assert params["resultType"] == "core"


def test_fetch_calls_http_get_json_with_expected_args(mocker, ing, epmc_url):
    fake = {"resultList": {"result": []}}
    http_get_json = mocker.patch.object(ing, "http_get_json", return_value=fake)

    out = ing.fetch(query="graphene", page=2, page_size=10, result_type="core")
    assert out == fake

    called_url = http_get_json.call_args.args[0]
    kwargs = http_get_json.call_args.kwargs
    assert called_url == epmc_url
    assert kwargs["params"]["query"] == "graphene"
    assert kwargs["params"]["page"] == 2
    assert kwargs["params"]["pageSize"] == 10
    assert kwargs["params"]["resultType"] == "core"


def test_fetch_propagates_http_error(mocker, ing, epmc_url):
    mocker.patch.object(
        ing, "http_get_json", side_effect=requests.HTTPError("upstream")
    )
    with pytest.raises(requests.HTTPError):
        ing.fetch(query="anything")


def test_fetch_async_calls_async_http_with_expected_args(mocker, ing, epmc_url):
    http_get_json_async = mocker.patch.object(
        ing,
        "http_get_json_async",
        new=AsyncMock(return_value={"resultList": {"result": []}}),
    )
    out = asyncio.run(ing.fetch_async(query="perovskite", page=4, page_size=5))
    assert out == {"resultList": {"result": []}}

    called_url = http_get_json_async.call_args.args[0]
    kwargs = http_get_json_async.call_args.kwargs
    assert called_url == epmc_url
    assert kwargs["params"]["query"] == "perovskite"
    assert kwargs["params"]["page"] == 4
    assert kwargs["params"]["pageSize"] == 5
    # result_type par défaut = "lite"
    assert kwargs["params"]["resultType"] == "lite"


def test_parse_builds_dataframe_with_expected_columns(ing):
    raw = {
        "resultList": {
            "result": [
                {
                    "id": "123",
                    "title": "A paper",
                    "source": "MED",
                    "pubYear": "2024",
                    "doi": "10.1234/abcd",
                    "authorString": "Doe J; Smith A",
                    "journalTitle": "J. Test",
                },
                {
                    "id": "456",
                    "title": "Another paper",
                    "source": "PAT",
                    "pubYear": "2023",
                    "authorString": "Roe B",
                    "journalTitle": "J. Test 2",
                },
            ]
        }
    }
    df = ing.parse(raw)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == [
        "id",
        "title",
        "source",
        "pub_year",
        "doi",
        "author_string",
        "journal_title",
        "text",
    ]
    assert len(df) == 2
    row0 = df.iloc[0].to_dict()
    assert row0["id"] == "123"
    assert row0["title"] == "A paper"
    assert row0["pub_year"] == "2024"
    assert row0["doi"] == "10.1234/abcd"
    assert row0["author_string"] == "Doe J; Smith A"
    assert row0["journal_title"] == "J. Test"
    assert row0["text"] == "A paper"


def test_parse_empty_returns_empty_dataframe(ing):
    assert ing.parse({"resultList": {"result": []}}).empty
    assert ing.parse({}).empty

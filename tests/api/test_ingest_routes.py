import asyncio
from unittest.mock import AsyncMock
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from app.api.routes.ingest import router, run_one


@pytest.fixture(scope="module")
def test_client():
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def test_run_one_text_ok(mocker):
    mp = mocker.patch("app.api.routes.ingest.EuropePMCIngestor")
    inst = mp.return_value
    inst.run_async = mocker.AsyncMock(return_value=[{"id": 1}, {"id": 2}, {"id": 3}])

    out = asyncio.run(run_one("text"))
    assert out == {"rows": 3}
    inst.run_async.assert_awaited_once_with(
        query="materials science", page=1, page_size=25
    )


def test_run_one_simulation_ok(mocker):
    mp = mocker.patch("app.api.routes.ingest.MaterialsProjectIngestor")
    inst = mp.return_value
    inst.run_async = mocker.AsyncMock(return_value=[{"m": "a"}, {"m": "b"}])

    out = asyncio.run(run_one("simulation"))
    assert out == {"rows": 2}
    inst.run_async.assert_awaited_once_with(formula="Si", per_page=10)


def test_run_one_experimental_ok(mocker):
    ts = mocker.patch("app.api.routes.ingest.TimeSeriesIngestor")
    inst = ts.return_value
    inst.run_async = mocker.AsyncMock(return_value=[{"t": 0}, {"t": 1}])

    out = asyncio.run(run_one("experimental"))
    assert out == {"rows": 2}
    inst.run_async.assert_awaited_once_with(path="data/raw/example_timeseries.csv")


def test_run_one_invalid_source_raises():
    with pytest.raises(HTTPException) as exc:
        asyncio.run(run_one("nope"))
    assert exc.value.status_code == 400
    assert "Invalid source" in exc.value.detail


def test_ingest_single_text_success(mocker, test_client: TestClient):
    ep = mocker.patch("app.api.routes.ingest.EuropePMCIngestor")
    inst = ep.return_value
    inst.run_async = mocker.AsyncMock(return_value=[1, 2, 3])

    resp = test_client.post("/ingest/text")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert body["source"] == "text"
    assert body["result"] == {"rows": 3}


def test_ingest_invalid_source_returns_400(test_client: TestClient):
    resp = test_client.post("/ingest/unknown")
    assert resp.status_code == 400
    body = resp.json()
    assert "Invalid source" in body["detail"]


def test_ingest_all_mixed_results(mocker, test_client: TestClient):
    mocker.patch(
        "app.api.routes.ingest.run_one",
        new=AsyncMock(
            side_effect=[
                {"rows": 5},
                Exception("boom"),
                {"rows": 1},
            ]
        ),
    )

    resp = test_client.post("/ingest/all")
    assert resp.status_code == 200
    body = resp.json()

    assert body["status"] == "completed"
    results = body["results"]

    assert results["text"]["status"] == "success"
    assert results["text"]["result"] == {"rows": 5}

    assert results["simulation"]["status"] == "error"
    assert "boom" in results["simulation"]["message"]

    assert results["experimental"]["status"] == "success"
    assert results["experimental"]["result"] == {"rows": 1}


def test_ingest_all_handles_exceptions_object(mocker, test_client: TestClient):
    mocker.patch(
        "app.api.routes.ingest.run_one",
        new=AsyncMock(side_effect=[Exception("x"), Exception("y"), Exception("z")]),
    )

    resp = test_client.post("/ingest/all")
    assert resp.status_code == 200
    body = resp.json()
    assert body["results"]["text"]["status"] == "error"
    assert body["results"]["simulation"]["status"] == "error"
    assert body["results"]["experimental"]["status"] == "error"

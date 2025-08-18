import asyncio

from fastapi import APIRouter, HTTPException

from app.core.logging_factory import LoggerFactory
from app.ingestion.simulation_ingestor import MaterialsProjectIngestor
from app.ingestion.text_ingestor import EuropePMCIngestor
from app.ingestion.timeseries_ingestor import TimeSeriesIngestor
from app.core.db import PostgresClient
from app.core.s3 import S3Client
from app.core.registry import Registry

router = APIRouter(prefix="/ingest", tags=["Ingestion"])
logger = LoggerFactory.get_logger(__name__)

INGESTORS = {
    "text": EuropePMCIngestor,
    "simulation": MaterialsProjectIngestor,
    "experimental": TimeSeriesIngestor,
}

db = PostgresClient()
s3 = S3Client()
registry = Registry(db, s3)
registry.bootstrap()


async def run_one(source: str) -> dict:
    """
    Instantiate the appropriate ingestor and run the asynchronous fetch and parse sequence.

    Args:
        source (str): The source type to ingest from. Must be one of 'text', 'simulation', or 'experimental'.

    Returns:
        dict: A dictionary containing the number of rows ingested.

    Raises:
        HTTPException: If the source is invalid.
    """
    if source == "text":
        ing = EuropePMCIngestor(registry=registry)
        raw = await ing.run_async(query="materials science", page=1, page_size=25)
        return {"rows": len(raw)}
    elif source == "simulation":
        ing = MaterialsProjectIngestor(registry=registry)
        raw = await ing.run_async(formula="Si", per_page=10)
        return {"rows": len(raw)}
    elif source == "experimental":
        ing = TimeSeriesIngestor(registry=registry)
        raw = await ing.run_async(path="data/raw/example_timeseries.csv")
        return {"rows": len(raw)}
    else:
        raise HTTPException(status_code=400, detail=f"Invalid source: {source}")


@router.post("/{source}")
async def ingest_source(source: str):
    """
    Trigger ingestion for a given source or for all sources.

    Args:
        source (str): The source to ingest from. Can be 'text', 'simulation', 'experimental', or 'all'.

    Returns:
        dict: A dictionary containing the status and results of the ingestion process.

    Raises:
        HTTPException: For invalid source or internal ingestion errors.
    """
    logger.info("Ingestion request received", extra={"source": source})

    try:
        if source == "all":
            names = list(INGESTORS.keys())
            coros = [run_one(n) for n in names]
            results = await asyncio.gather(*coros, return_exceptions=True)

            summary = {}
            for n, r in zip(names, results):
                if isinstance(r, Exception):
                    logger.error(
                        "Ingestion failed", extra={"source": n, "error": str(r)}
                    )
                    summary[n] = {"status": "error", "message": str(r)}
                else:
                    summary[n] = {"status": "success", "result": r}
            return {"status": "completed", "results": summary}

        if source not in INGESTORS:
            raise HTTPException(status_code=400, detail=f"Invalid source: {source}")

        result = await run_one(source)
        return {"status": "success", "source": source, "result": result}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Ingestion failed", extra={"source": source, "error": str(e)})
        raise HTTPException(status_code=500, detail=str(e))

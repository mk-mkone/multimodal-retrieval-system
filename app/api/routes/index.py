from fastapi import APIRouter, HTTPException, Depends
from typing import Optional, Dict, Any
from app.indexing.engine import HybridSearchEngine
from app.indexing.schemas import SearchRequest, SearchResponse, MaterialResponse
from app.core.logging_factory import LoggerFactory
from app.core.config import settings
import psycopg

router = APIRouter(prefix="/search", tags=["search"])
logger = LoggerFactory.get_logger(__name__)

_engine_singleton: Optional[HybridSearchEngine] = None


def get_engine() -> HybridSearchEngine:
    global _engine_singleton
    if _engine_singleton is None:
        _engine_singleton = HybridSearchEngine()
    return _engine_singleton


@router.post("", response_model=SearchResponse)
def search(
    req: SearchRequest, engine: HybridSearchEngine = Depends(get_engine)
) -> SearchResponse:
    """
    Perform a top-k semantic search for one modality (MVP supports 'text').
    """
    try:
        return engine.search(req)
    except ValueError as e:
        logger.warning("search_bad_request", extra={"error": str(e)})
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:  # pragma: no cover
        logger.error("search_failed", extra={"error": str(e)})
        raise HTTPException(status_code=500, detail="search failed")


@router.get("/materials/{doc_id}", response_model=MaterialResponse)
def get_material(doc_id: str) -> MaterialResponse:
    """
    Fetch a single document's metadata by id from Postgres.
    """
    sql = """
        SELECT id, metadata
        FROM documents
        WHERE id = %s
    """
    with psycopg.connect(settings.POSTGRES_URI) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (doc_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="not found")
            return MaterialResponse(doc_id=row[0], metadata=row[1])


@router.get("/metadata")
def list_metadata_fields() -> Dict[str, Any]:
    """
    Return simple aggregations to help clients build filters (MVP).
    """
    sql = """
      SELECT
        json_build_object(
          'years', COALESCE((SELECT array_agg(DISTINCT year ORDER BY year) FROM documents WHERE year IS NOT NULL), ARRAY[]::int[]),
          'methods', COALESCE((SELECT array_agg(DISTINCT method ORDER BY method) FROM documents WHERE method IS NOT NULL), ARRAY[]::text[])
        )
    """
    with psycopg.connect(settings.POSTGRES_URI) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            (payload,) = cur.fetchone()
            return payload or {"years": [], "methods": []}

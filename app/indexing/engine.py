from typing import List, Tuple, Optional, Dict, Any
import time
import numpy as np
import psycopg

from app.indexing.faiss_index import load_faiss_index
from app.embedding.text_sbert import SbertTextEngine
from app.indexing.schemas import SearchRequest, SearchResponse, SearchHit
from app.core.config import settings
from app.core.logging_factory import LoggerFactory

logger = LoggerFactory.get_logger(__name__)


DEFAULT_TEXT_MODEL = "all-MiniLM-L6-v2"


class FaissSearchBackend:
    """
    Wrapper around a FAISS index + ids mapping.
    """

    def __init__(self, index_dir: str, *, kind: str, model: str):
        self.kind = kind
        self.model = model
        self.index, self.ids = load_faiss_index(index_dir, kind=kind, model=model)

    def search(
        self, query_vec: np.ndarray, top_k: int = 10
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Perform top-k search on the FAISS index.

        Returns
        -------
        (idxs, scores)
          idxs   : int64 indices into ids array (shape (1, top_k))
          scores : float32 scores (shape (1, top_k))
        """
        if query_vec.ndim == 1:
            query_vec = query_vec[None, :]
        D, I = self.index.search(query_vec.astype(np.float32), top_k)
        return I[0], D[0]


def _encode_text_query(query: str, model_name: Optional[str]) -> Tuple[str, np.ndarray]:
    """
    Encode a free-text query using SbertTextEngine.

    Returns
    -------
    (used_model_name, vector)
    """
    eng = SbertTextEngine(
        model_name=f"sentence-transformers/{model_name or DEFAULT_TEXT_MODEL}"
    )
    vec = eng.embed_batch([{"text": query}])
    return eng.name, vec[0]


def _fetch_metadata(
    doc_ids: List[str], filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch metadata from Postgres for a list of doc_ids and optional filters.
    """
    if not doc_ids:
        return {}

    f_year_from = filters.get("year_from") if filters else None
    f_year_to = filters.get("year_to") if filters else None
    f_method = filters.get("method") if filters else None

    clauses = ["id = ANY(%s)"]
    params: List[Any] = [doc_ids]
    if f_year_from is not None:
        clauses.append("COALESCE(year, 0) >= %s")
        params.append(int(f_year_from))
    if f_year_to is not None:
        clauses.append("COALESCE(year, 9999) <= %s")
        params.append(int(f_year_to))
    if f_method:
        clauses.append("method = %s")
        params.append(f_method)

    where = " AND ".join(clauses)
    sql = f"""
        SELECT id, kind, source, source_id, year, method,
               (metadata->>'title') AS title
        FROM documents
        WHERE {where}
    """

    rows: Dict[str, Dict[str, Any]] = {}

    with psycopg.connect(str(settings.POSTGRES_URI)) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for r in cur.fetchall():
                doc_id = r[0]
                rows[doc_id] = {
                    "doc_id": r[0],
                    "kind": r[1],
                    "source": r[2],
                    "source_id": r[3],
                    "year": r[4],
                    "method": r[5],
                    "title": r[6],
                }
    return rows


class HybridSearchEngine:
    """
    Hybrid search MVP: FAISS retrieval per modality + metadata fetch from Postgres.

    For now, one modality per request (text|simulation|timeseries).
    """

    def __init__(
        self, emb_root: str = "data/embeddings", index_dir: str = "data/index/faiss"
    ):
        self.emb_root = emb_root
        self.index_dir = index_dir
        self._faiss_cache: Dict[Tuple[str, str], FaissSearchBackend] = {}

    def _get_backend(self, *, kind: str, model: str) -> FaissSearchBackend:
        key = (kind, model)
        if key not in self._faiss_cache:
            self._faiss_cache[key] = FaissSearchBackend(
                self.index_dir, kind=kind, model=model
            )
        return self._faiss_cache[key]

    def search(self, req: SearchRequest) -> SearchResponse:
        t0 = time.time()

        if req.kind == "text":
            used_model, qvec = _encode_text_query(req.query or "", req.model)
            model = used_model
        else:
            raise ValueError("Only 'text' search is implemented in MVP")

        backend = self._get_backend(kind=req.kind, model=model)
        idxs, scores = backend.search(qvec, top_k=max(req.top_k, req.page * req.size))
        doc_ids = [str(backend.ids[i]) for i in idxs if i >= 0]

        meta_map = _fetch_metadata(
            doc_ids, filters=req.filters.model_dump() if req.filters else None
        )

        kept: List[SearchHit] = []
        for di, sc in zip(doc_ids, scores[: len(doc_ids)]):
            m = meta_map.get(di)
            if not m:
                continue
            kept.append(
                SearchHit(
                    doc_id=di,
                    score=float(sc),
                    title=m.get("title"),
                    year=m.get("year"),
                    source=m.get("source"),
                    kind=m.get("kind"),
                    method=m.get("method"),
                )
            )

        total = len(kept)
        start = (req.page - 1) * req.size
        end = start + req.size
        items = kept[start:end]

        logger.info(
            "search_done",
            extra={
                "kind": req.kind,
                "model": model,
                "q_len": len(req.query or ""),
                "top_k": req.top_k,
                "page": req.page,
                "size": req.size,
                "results": len(items),
                "total_after_filters": total,
                "latency_ms": int((time.time() - t0) * 1000),
            },
        )

        return SearchResponse(total=total, page=req.page, size=req.size, items=items)

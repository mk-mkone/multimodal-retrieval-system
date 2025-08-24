from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class SearchFilters(BaseModel):
    """
    Optional metadata filters applied after the FAISS top-k stage.
    Extend as needed (method, elements, etc.).
    """

    year_from: Optional[int] = None
    year_to: Optional[int] = None
    method: Optional[str] = None


class SearchRequest(BaseModel):
    """
    Request payload for /search.
    For 'text' kind, 'query' is required.
    For 'simulation' or 'timeseries', you may pass a precomputed vector later;
    MVP uses only 'text' query for simplicity.
    """

    kind: str = Field(pattern="^(text|simulation|timeseries)$")
    query: Optional[str] = None
    model: Optional[str] = None
    top_k: int = 10
    page: int = 1
    size: int = 10
    filters: Optional[SearchFilters] = None


class SearchHit(BaseModel):
    doc_id: str
    score: float
    title: Optional[str] = None
    year: Optional[int] = None
    source: Optional[str] = None
    kind: Optional[str] = None
    method: Optional[str] = None


class SearchResponse(BaseModel):
    total: int
    page: int
    size: int
    items: List[SearchHit]


class MaterialResponse(BaseModel):
    doc_id: str
    metadata: Dict[str, Any]

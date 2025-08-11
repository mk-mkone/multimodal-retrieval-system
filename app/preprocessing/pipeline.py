from typing import List, Dict, Any
from app.preprocessing.adapters import to_textdoc, to_simdoc, to_tsdoc
from app.models.pivot import TextDoc, SimulationDoc, TimeSeriesDoc


def preprocess_text(rows: List[Dict[str, Any]]) -> List[TextDoc]:
    """Takes a list of dicts representing raw text records and returns a list of TextDoc objects via to_textdoc."""
    return [to_textdoc(r) for r in rows]


def preprocess_sim(items: List[Dict[str, Any]]) -> List[SimulationDoc]:
    """Takes a list of dicts representing raw simulation data and returns a list of SimulationDoc objects via to_simdoc."""
    return [to_simdoc(x) for x in items]


def preprocess_timeseries(items: List[Dict[str, Any]]) -> List[TimeSeriesDoc]:
    """Takes a list of dicts representing raw time series data and returns a list of TimeSeriesDoc objects via to_tsdoc."""
    return [to_tsdoc(x) for x in items]

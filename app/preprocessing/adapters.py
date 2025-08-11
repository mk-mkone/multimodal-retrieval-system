from datetime import datetime, timezone
from typing import Any, Dict
from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError

from app.models.pivot import MaterialIdentity, SimulationDoc, TextDoc, TimeSeriesDoc
from app.preprocessing.normalize import (
    canonicalize_formula,
    material_hash_from_formula,
    parse_date_any,
    to_s,
)


def _mk_uid(namespace: str, external_id: str) -> str:
    """Create a deterministic UUID5 based on a namespace and external identifier."""
    return str(uuid5(NAMESPACE_URL, f"{namespace}:{external_id}"))


def to_textdoc(row: Dict[str, Any], source: str = "europepmc") -> TextDoc:
    """Map a raw text record (e.g., from EuropePMC) into a TextDoc, including normalization of date, title, and authors."""
    title = row.get("title") or ""
    text = row.get("text") or title
    year = row.get("pub_year") or row.get("year")
    created_at = parse_date_any(year or datetime.now(timezone.utc))
    material = None
    try:
        return TextDoc(
            uid=_mk_uid(source, str(row.get("id") or row.get("doi") or title[:50])),
            source=source,
            source_id=str(row.get("id") or row.get("doi") or ""),
            created_at=created_at,
            title=title or None,
            text=text,
            year=int(year) if year else None,
            authors=(
                row.get("authors") or (row.get("author_string") or "").split("; ")
                if row.get("author_string")
                else None
            ),
            venue=row.get("venue") or row.get("journal_title"),
            material=material,
        )
    except ValidationError:
        raise


def to_simdoc(item: Dict[str, Any], source: str = "materials_project") -> SimulationDoc:
    """Map a raw simulation record (e.g., from Materials Project) into a SimulationDoc, performing
    canonicalization of chemical formulas and normalizing properties like band gap and density."""
    formula = item.get("formula") or item.get("formula_pretty") or "X"
    canonical, elements = canonicalize_formula(formula)
    mid = item.get("material_id") or canonical
    props = {}
    if (
        bg := item.get("band_gap") or (item.get("bandstructure") or {}).get("band_gap")
    ) is not None:
        props["band_gap_eV"] = float(bg)
    if (rho := item.get("density")) is not None:
        props["density_g_cm3"] = float(rho)
    created_at = parse_date_any(item.get("year") or datetime.now(timezone.utc))
    return SimulationDoc(
        uid=_mk_uid(source, str(mid)),
        source=source,
        source_id=str(mid),
        created_at=created_at,
        method=item.get("method") or item.get("functional"),
        material=MaterialIdentity(
            formula=formula,
            canonical_formula=canonical,
            elements=elements,
            n_elements=len(elements),
            material_hash=material_hash_from_formula(formula),
        ),
        properties=props,
        references=item.get("references"),
    )


def to_tsdoc(
    payload: Dict[str, Any], source: str = "local_timeseries"
) -> TimeSeriesDoc:
    """Map a raw time series payload (e.g., spectra data) into a TimeSeriesDoc, performing normalization
    of time units and chemical formulas for the associated material identity."""
    units = payload.get("units") or {}
    values = payload.get("values") or []
    if values and "t_unit" in payload:
        conv = lambda t: to_s(float(t), payload["t_unit"])
        values = [
            {
                "t": conv(v.get("t", v.get("x", 0))),
                "v": float(v.get("v", v.get("y", 0))),
            }
            for v in values
        ]
        units["t"] = "s"
    material = None
    if m := payload.get("material", {}).get("formula"):
        canonical, elements = canonicalize_formula(m)
        material = MaterialIdentity(
            formula=m,
            canonical_formula=canonical,
            elements=elements,
            n_elements=len(elements),
            material_hash=material_hash_from_formula(m),
        )
    return TimeSeriesDoc(
        uid=_mk_uid(source, payload.get("path", "unknown")),
        source=source,
        source_id=payload.get("path"),
        created_at=parse_date_any(
            payload.get("created_at") or datetime.now(timezone.utc)
        ),
        modality=payload.get("modality", "spectra"),
        units=units,
        values=[{"t": float(v["t"]), "v": float(v["v"])} for v in values],
        instrument=payload.get("instrument"),
        conditions=payload.get("conditions"),
        material=material,
    )

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, field_validator


class MaterialIdentity(BaseModel):
    """
    Represents a common material identity with chemical formula information.

    Fields:
    - formula: Original chemical formula, e.g., "SiO2".
    - canonical_formula: Alphabetically sorted formula, e.g., "O2Si".
    - elements: List of chemical element symbols, e.g., ["O", "Si"].
    - n_elements: Number of distinct elements.
    - material_hash: Stable hash identifier for the material.
    """

    formula: str
    canonical_formula: str
    elements: List[str]
    n_elements: int
    material_hash: str

    @field_validator("elements")
    @classmethod
    def _upper_sort(cls, v: List[str]) -> List[str]:
        return sorted([e.capitalize() for e in v])


class BaseDoc(BaseModel):
    """
    Base class for all document types with common metadata fields.

    Fields:
    - uid: Unique identifier (uuid5 or hash).
    - source: Data source name, e.g., "europepmc", "materials_project".
    - source_id: Optional external identifier such as DOI or file path.
    - created_at: Timestamp of data acquisition (UTC normalized).
    - version: Internal preprocessing version, default "v1".
    """

    uid: str
    source: str
    source_id: Optional[str] = None
    created_at: datetime
    version: str = "v1"


class TextDoc(BaseDoc):
    """
    Document representing textual data.

    Fields:
    - kind: Literal type discriminator, fixed to "text".
    - title: Optional document title.
    - text: Full text content.
    - year: Optional publication or creation year.
    - authors: Optional list of authors.
    - venue: Optional publication venue or source.
    - material: Optional associated material identity.
    """

    kind: Literal["text"] = "text"
    title: Optional[str] = None
    text: str
    year: Optional[int] = None
    authors: Optional[List[str]] = None
    venue: Optional[str] = None
    material: Optional[MaterialIdentity] = None


class SimulationDoc(BaseDoc):
    """
    Document representing simulation data related to a material.

    Fields:
    - kind: Literal type discriminator, fixed to "simulation".
    - material: Material identity for the simulation.
    - method: Optional simulation method description, e.g., "DFT-PBE".
    - properties: Dictionary of simulation properties and their values.
    - references: Optional list of reference identifiers.
    """

    kind: Literal["simulation"] = "simulation"
    material: MaterialIdentity
    method: Optional[str] = None
    properties: Dict[str, Any] = {}
    references: Optional[List[str]] = None


class TimeSeriesDoc(BaseDoc):
    """
    Document representing timeseries data with associated metadata.

    Fields:
    - kind: Literal type discriminator, fixed to "timeseries".
    - modality: Type of timeseries data, e.g., "spectra", "chrono", or "other".
    - units: Units for the data axes, e.g., {"x": "s", "y": "a.u."}.
    - values: List of data points as dictionaries of float values.
    - instrument: Optional instrument used for data acquisition.
    - conditions: Optional experimental or measurement conditions.
    - material: Optional associated material identity.
    """

    kind: Literal["timeseries"] = "timeseries"
    modality: Literal["spectra", "chrono", "other"] = "spectra"
    units: Dict[str, str]
    values: List[Dict[str, float]]
    instrument: Optional[str] = None
    conditions: Optional[Dict[str, Any]] = None
    material: Optional[MaterialIdentity] = None

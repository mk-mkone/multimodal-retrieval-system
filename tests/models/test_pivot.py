import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.models.pivot import (
    MaterialIdentity,
    TextDoc,
    SimulationDoc,
    TimeSeriesDoc,
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def test_material_identity_capitalizes_and_sorts_elements():
    mi = MaterialIdentity(
        formula="siO2",
        canonical_formula="O2Si",
        elements=["si", "O"],
        n_elements=2,
        material_hash="abc123",
    )
    assert mi.elements == ["O", "Si"]
    assert mi.n_elements == 2
    assert mi.canonical_formula == "O2Si"


def test_material_identity_invalid_elements_type_raises():
    with pytest.raises(ValidationError):
        MaterialIdentity(
            formula="H2O",
            canonical_formula="H2O",
            elements="H,O",
            n_elements=2,
            material_hash="xyz",
        )


def test_textdoc_minimal_fields_and_defaults():
    td = TextDoc(
        uid="u1",
        source="europepmc",
        created_at=now_utc(),
        text="hello",
    )
    assert td.kind == "text"
    assert td.version == "v1"
    assert td.title is None
    assert td.year is None
    assert td.material is None


def test_textdoc_serialization_contains_expected_fields():
    td = TextDoc(
        uid="u2",
        source="src",
        created_at=now_utc(),
        text="content",
        title="Title",
        year=2024,
    )
    data = json.loads(td.model_dump_json())
    assert data["kind"] == "text"
    assert data["text"] == "content"
    assert data["title"] == "Title"
    assert data["version"] == "v1"


def test_simulationdoc_requires_material():
    with pytest.raises(ValidationError):
        SimulationDoc(
            uid="s1",
            source="materials_project",
            created_at=now_utc(),
            properties={"band_gap_eV": 1.1},
        )


def test_simulationdoc_with_material_ok():
    mat = MaterialIdentity(
        formula="Si",
        canonical_formula="Si",
        elements=["si"],
        n_elements=1,
        material_hash="hash",
    )
    sd = SimulationDoc(
        uid="s2",
        source="mp",
        created_at=now_utc(),
        material=mat,
        method="DFT-PBE",
        properties={"band_gap_eV": 1.12},
        references=["ref1"],
    )
    assert sd.kind == "simulation"
    assert sd.properties["band_gap_eV"] == 1.12
    assert sd.material.elements == ["Si"]


def test_timeseriesdoc_defaults_and_types():
    ts = TimeSeriesDoc(
        uid="t1",
        source="local",
        created_at=now_utc(),
        units={"x": "s", "y": "a.u."},
        values=[{"t": 0.0, "v": 1.0}, {"t": 1.0, "v": 2.0}],
    )
    assert ts.kind == "timeseries"
    assert ts.modality == "spectra"
    assert isinstance(ts.values, list)
    assert ts.values[1]["v"] == 2.0


def test_timeseriesdoc_invalid_units_type_raises():
    with pytest.raises(ValidationError):
        TimeSeriesDoc(
            uid="t2",
            source="local",
            created_at=now_utc(),
            units=[("x", "s")],
            values=[{"t": 0.0, "v": 1.0}],
        )


def test_timeseriesdoc_invalid_values_type_raises():
    with pytest.raises(ValidationError):
        TimeSeriesDoc(
            uid="t3",
            source="local",
            created_at=now_utc(),
            units={"x": "s"},
            values={"t": 0.0, "v": 1.0},
        )

from datetime import datetime, timezone
from uuid import UUID

import pytest

from app.preprocessing.adapters import _mk_uid, to_textdoc, to_simdoc, to_tsdoc


def dt_utc(y=2024, m=1, d=1):
    return datetime(y, m, d, tzinfo=timezone.utc)


def test_mk_uid_is_deterministic():
    u1 = _mk_uid("ns", "ext-1")
    u2 = _mk_uid("ns", "ext-1")
    assert u1 == u2
    assert UUID(u1).version == 5


def test_to_textdoc_minimal_maps_fields_and_year():
    row = {
        "id": "EPMC-1",
        "title": "A study of graphene",
        "pub_year": "2024",
    }
    doc = to_textdoc(row, source="europepmc")
    assert doc.uid
    assert doc.source == "europepmc"
    assert doc.source_id == "EPMC-1"
    assert doc.title == "A study of graphene"
    assert doc.text == "A study of graphene"
    assert doc.year == 2024
    assert doc.kind == "text"


def test_to_textdoc_parses_authors_and_venue():
    row = {
        "id": "2",
        "title": "Battery materials",
        "pub_year": "2023",
        "author_string": "Doe J; Smith A",
        "journal_title": "J. Energy",
    }
    doc = to_textdoc(row)
    assert doc.authors == ["Doe J", "Smith A"]
    assert doc.venue == "J. Energy"


def test_to_simdoc_maps_properties_and_material_identity():
    item = {
        "material_id": "mp-149",
        "formula_pretty": "Si",
        "band_gap": 1.12,
        "density": 2.33,
        "year": "2022",
        "references": ["ref1"],
    }
    doc = to_simdoc(item)
    assert doc.kind == "simulation"
    assert doc.source_id == "mp-149"
    assert doc.properties["band_gap_eV"] == pytest.approx(1.12)
    assert doc.properties["density_g_cm3"] == pytest.approx(2.33)
    assert doc.material.formula == "Si"
    assert doc.material.canonical_formula == "Si"
    assert doc.material.elements == ["Si"]


def test_to_simdoc_band_gap_from_nested_bandstructure():
    item = {
        "material_id": "mp-13",
        "formula": "C",
        "bandstructure": {"band_gap": 5.5},
    }
    doc = to_simdoc(item)
    assert doc.properties["band_gap_eV"] == pytest.approx(5.5)


def test_to_tsdoc_time_unit_conversion_ms_to_s():
    payload = {
        "path": "sig.csv",
        "t_unit": "ms",
        "values": [{"t": 0, "v": 1.0}, {"t": 10, "v": 2.0}],
        "units": {"y": "a.u."},
    }
    doc = to_tsdoc(payload)
    assert doc.values[1]["t"] == pytest.approx(0.01)
    assert doc.units.get("t") == "s"


def test_to_tsdoc_with_material_identity():
    payload = {
        "path": "p.csv",
        "values": [{"t": 0, "v": 1.0}],
        "material": {"formula": "SiO2"},
    }
    doc = to_tsdoc(payload, source="local_timeseries")
    assert doc.source == "local_timeseries"
    assert doc.material is not None
    assert doc.material.canonical_formula == "O2Si"
    assert sorted(doc.material.elements) == ["O", "Si"]


def test_to_tsdoc_defaults_when_minimal():
    payload = {"path": "a.csv", "values": []}
    doc = to_tsdoc(payload)
    assert doc.modality == "spectra"
    assert isinstance(doc.values, list)
    assert doc.values == []

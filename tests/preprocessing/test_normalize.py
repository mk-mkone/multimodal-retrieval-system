from datetime import datetime, timezone, timedelta

import pytest

from app.preprocessing.normalize import (
    parse_date_any,
    canonicalize_formula,
    material_hash_from_formula,
    to_eV,
    to_K,
    to_s,
)


def test_parse_date_any_accepts_datetime_and_normalizes_utc():
    naive = datetime(2024, 1, 2, 3, 4, 5)  # naive
    dt = parse_date_any(naive)
    assert dt.tzinfo is not None
    assert dt.tzinfo.utcoffset(dt) == timezone.utc.utcoffset(dt)


def test_parse_date_any_parses_various_formats():
    d1 = parse_date_any("2024-03-15")
    assert d1.year == 2024 and d1.month == 3 and d1.day == 15

    d2 = parse_date_any("2024/03")
    assert d2.year == 2024 and d2.month == 3 and d2.day == 1

    d3 = parse_date_any("2024")
    assert d3.year == 2024 and d3.month == 1 and d3.day == 1


def test_parse_date_any_parses_timestamps_seconds_and_ms():
    ts = 1_700_000_000
    d_s = parse_date_any(ts)
    assert d_s.tzinfo is not None

    ts_ms = (1_700_000_000) * 1000
    d_ms = parse_date_any(ts_ms)
    assert d_ms.tzinfo is not None


def test_parse_date_any_iso_and_fallback():
    iso = "2024-01-02T03:04:05Z"
    d = parse_date_any(iso)
    assert d.year == 2024 and d.hour == 3

    before = datetime.now(timezone.utc) - timedelta(seconds=2)
    d_bad = parse_date_any("not a date")
    after = datetime.now(timezone.utc) + timedelta(seconds=2)
    assert before <= d_bad <= after


def test_canonicalize_formula_sorts_and_omits_unity():
    canon, elems = canonicalize_formula("SiO2")
    assert canon == "O2Si"
    assert elems == ["O", "Si"]

    canon2, elems2 = canonicalize_formula("C")
    assert canon2 == "C"
    assert elems2 == ["C"]


def test_canonicalize_formula_handles_floats():
    canon, elems = canonicalize_formula("Fe1.5O")
    assert canon == "Fe1.5O"
    assert elems == ["Fe", "O"]


def test_canonicalize_formula_accumulates_counts():
    canon, elems = canonicalize_formula("H2O H2")
    assert canon == "H4O"


def test_material_hash_is_stable_and_short():
    h1 = material_hash_from_formula("SiO2")
    h2 = material_hash_from_formula("SiO2")
    assert h1 == h2
    assert isinstance(h1, str) and len(h1) == 16
    assert h1 != material_hash_from_formula("Si")


def test_to_eV_supported_units():
    assert to_eV(1.0, "eV") == 1.0
    # 96.485 kJ/mol ≈ 1 eV (approximation declared in code)
    assert to_eV(96.485, "kJ/mol") == pytest.approx(1.0, rel=1e-6)
    # 1 J = 1 / 1.602176634e-19 eV
    assert to_eV(1.602176634e-19, "J") == pytest.approx(1.0, rel=1e-12)


def test_to_eV_unsupported_raises():
    with pytest.raises(ValueError):
        to_eV(1.0, "cal")


def test_to_K_supported_units():
    assert to_K(300, "K") == 300
    assert to_K(0, "C") == pytest.approx(273.15)
    assert to_K(32, "F") == pytest.approx(273.15)


def test_to_K_unsupported_raises():
    with pytest.raises(ValueError):
        to_K(100, "Rankine")


def test_to_s_supported_units():
    assert to_s(1.0, "s") == 1.0
    assert to_s(10.0, "ms") == 0.01
    assert to_s(10.0, "us") == 10.0 / 1e6
    assert to_s(10.0, "ns") == 10.0 / 1e9
    assert to_s(2.0, "min") == 120.0
    assert to_s(1.0, "h") == 3600.0


def test_to_s_unsupported_raises():
    with pytest.raises(ValueError):
        to_s(1.0, "day")

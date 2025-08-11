import hashlib
import re
from datetime import datetime, timezone
from typing import Dict, List, Tuple


def parse_date_any(s: str | int | float | datetime) -> datetime:
    """Robustly parse various common date/time formats or numeric timestamps into a UTC datetime object.

    Falls back to the current UTC time if parsing fails."""
    if isinstance(s, datetime):
        return s.astimezone(timezone.utc)
    if isinstance(s, (int, float)) and s > 10_000_000:
        return datetime.fromtimestamp(float(s) / 1000, tz=timezone.utc)
    if isinstance(s, (int, float)):
        return datetime.fromtimestamp(float(s), tz=timezone.utc)
    s = str(s).strip()

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m", "%Y/%m", "%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            if fmt == "%Y":
                dt = dt.replace(month=1, day=1)
            if fmt in ("%Y-%m", "%Y/%m"):
                dt = dt.replace(day=1)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


_element_re = re.compile(r"([A-Z][a-z]?)(\d*\.?\d*)")


def canonicalize_formula(formula: str) -> Tuple[str, List[str]]:
    """Normalize a chemical formula string by parsing element counts, sorting elements alphabetically,
    and returning the canonical formula string along with the sorted list of elements.
    """
    counts: Dict[str, float] = {}
    for el, num in _element_re.findall(formula.replace(" ", "")):
        n = float(num) if num else 1.0
        counts[el] = counts.get(el, 0.0) + n
    items = sorted(counts.items(), key=lambda kv: kv[0])
    parts = []
    for el, n in items:
        n_val = int(n) if float(n).is_integer() else n
        if (isinstance(n_val, int) and n_val == 1) or (
            not isinstance(n_val, int) and float(n) == 1.0
        ):
            parts.append(f"{el}")
        else:
            parts.append(f"{el}{n_val}")
    canonical = "".join(parts)
    elements = [el for el, _ in items]
    return canonical, elements


def material_hash_from_formula(formula: str) -> str:
    """Produce a short SHA256-based hash identifier from the canonicalized chemical formula,
    for use as a material identity key."""
    canonical, _ = canonicalize_formula(formula)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def to_eV(value: float, unit: str) -> float:
    """Convert a given energy value from supported units (eV, joules, kJ/mol) into electronvolts."""
    unit = unit.lower()
    if unit in ("ev",):
        return value
    if unit in ("j", "joule", "joules"):
        return value / 1.602176634e-19
    if unit in ("kj/mol", "kilojoule/mol"):  # approx: 1 eV ≈ 96.485 kJ/mol
        return value / 96.485
    raise ValueError(f"Unsupported energy unit: {unit}")


def to_K(value: float, unit: str) -> float:
    """Convert a given temperature from supported units (K, °C, °F) into kelvin."""
    u = unit.lower()
    if u in ("k", "kelvin"):
        return value
    if u in ("c", "°c", "celsius"):
        return value + 273.15
    if u in ("f", "°f", "fahrenheit"):
        return (value - 32) * 5 / 9 + 273.15
    raise ValueError(f"Unsupported temperature unit: {unit}")


def to_s(value: float, unit: str) -> float:
    """Convert a given time duration from supported units (seconds, ms, µs, ns, minutes, hours) into seconds."""
    u = unit.lower()
    if u in ("s", "sec", "second", "seconds"):
        return value
    if u in ("ms",):
        return value / 1e3
    if u in ("us", "µs"):
        return value / 1e6
    if u in ("ns",):
        return value / 1e9
    if u in ("min", "mins", "minute", "minutes"):
        return value * 60
    if u in ("h", "hr", "hour", "hours"):
        return value * 3600
    raise ValueError(f"Unsupported time unit: {unit}")

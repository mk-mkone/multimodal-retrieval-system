from typing import Iterable
import numpy as np

# Fixed order for stability
ELEMENTS = [
    "H",
    "He",
    "Li",
    "Be",
    "B",
    "C",
    "N",
    "O",
    "F",
    "Ne",
    "Na",
    "Mg",
    "Al",
    "Si",
    "P",
    "S",
    "Cl",
    "Ar",
    "K",
    "Ca",
]


class SimpleMaterialFingerprint:
    """
    MVP material embedding: normalized element histogram over the first 20 elements.

    This is a lightweight placeholder you can later swap for Coulomb Matrix,
    SOAP or graph-based embeddings (CGCNN).
    """

    def __init__(self):
        self.name = "element-hist-20"
        self.modality = "simulation"
        self.dim = len(ELEMENTS)

    def embed_batch(self, items: Iterable[dict]) -> np.ndarray:
        vecs = []
        for d in items:
            mat = d.get("material") or {}
            elems = mat.get("elements") or []
            counts = {e: elems.count(e) for e in set(elems)}
            v = np.zeros(self.dim, dtype=np.float32)
            total = float(sum(counts.values()) or 1.0)
            for i, el in enumerate(ELEMENTS):
                v[i] = counts.get(el, 0) / total
            vecs.append(v)
        return np.stack(vecs) if vecs else np.empty((0, self.dim), dtype=np.float32)

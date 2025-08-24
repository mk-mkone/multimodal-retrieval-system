from pathlib import Path
from typing import Literal
import json
import numpy as np


class EmbeddingStore:
    """
    Persist embedding parts to disk and maintain a small manifest.

    By default tries to write Parquet if pyarrow is installed, otherwise
    falls back to compressed .npz.
    """

    def __init__(self, root: str = "data/embeddings"):
        self.root = Path(root)

    def _dir(self, kind: str, model: str) -> Path:
        p = self.root / kind / model
        p.mkdir(parents=True, exist_ok=True)
        return p

    def save_part(
        self,
        *,
        kind: str,
        model: str,
        part: str,
        doc_ids: list[str],
        vectors: np.ndarray,
        fmt: Literal["auto", "parquet", "npz"] = "auto",
    ) -> Path:
        if vectors.ndim != 2:
            raise ValueError("vectors must be a 2D array (N, dim)")
        dim = int(vectors.shape[1])

        d = self._dir(kind, model)

        if fmt == "auto":
            try:
                import pyarrow as pa  # noqa: F401
                import pyarrow.parquet as pq  # noqa: F401

                fmt = "parquet"
            except Exception:
                fmt = "npz"

        if fmt == "parquet":
            out = d / f"{part}.parquet"
            self._write_parquet(out, doc_ids, vectors)
        else:
            out = d / f"{part}.npz"
            self._write_npz(out, doc_ids, vectors)

        self._update_manifest(
            d, kind, model, part, count=int(vectors.shape[0]), dim=dim
        )
        return out

    def _write_parquet(
        self, out: Path, doc_ids: list[str], vectors: np.ndarray
    ) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table(
            {
                "doc_id": pa.array(doc_ids, type=pa.string()),
                "vector": pa.array(vectors.tolist()),  # list<item: float32>
            }
        )
        pq.write_table(table, out)

    def _write_npz(self, out: Path, doc_ids: list[str], vectors: np.ndarray) -> None:
        np.savez_compressed(
            out, ids=np.array(doc_ids, dtype=object), vecs=vectors.astype(np.float32)
        )

    def _update_manifest(
        self, dir_path: Path, kind: str, model: str, part: str, *, count: int, dim: int
    ) -> None:
        man = dir_path / "manifest.json"
        entry = {"part": part, "count": count, "dim": dim}
        if man.exists():
            data = json.loads(man.read_text())
            data.setdefault("parts", []).append(entry)
        else:
            data = {"kind": kind, "model": model, "parts": [entry]}
        man.write_text(json.dumps(data, indent=2))

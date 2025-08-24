from pathlib import Path
from typing import Tuple, List, Dict, Any
import json
import numpy as np

try:
    import faiss
except Exception as exc:  # pragma: no cover
    faiss = None
    _faiss_import_err = exc
else:
    _faiss_import_err = None


def _ensure_faiss():
    if faiss is None:
        raise RuntimeError(f"faiss not available: {_faiss_import_err}")


def _load_manifest(dir_path: Path) -> Dict[str, Any]:
    man = dir_path / "manifest.json"
    if not man.exists():
        raise FileNotFoundError(f"Manifest not found: {man}")
    return json.loads(man.read_text(encoding="utf-8"))


def _load_part(path: Path) -> Tuple[np.ndarray, np.ndarray]:
    """
    Load a single embedding part file.

    Supports:
      - .npz  -> arrays 'ids' (object dtype) and 'vecs' (float32)
      - .parquet -> columns 'doc_id' (string) and 'vector' (list<float>)
    """
    if path.suffix == ".npz":
        data = np.load(path, allow_pickle=True)
        ids = data["ids"]
        vecs = data["vecs"].astype(np.float32, copy=False)
        return ids, vecs

    if path.suffix == ".parquet":
        import pyarrow.parquet as pq

        table = pq.read_table(path)
        ids = np.array(table["doc_id"].to_pylist(), dtype=object)
        vecs = np.asarray(table["vector"].to_pylist(), dtype=np.float32)
        return ids, vecs

    raise ValueError(f"Unsupported part format: {path.name}")


def load_embeddings_dir(
    root: str, kind: str, model: str
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Load and concatenate all embedding parts for a given (kind, model).

    Returns
    -------
    (ids, vecs)
      ids : np.ndarray of shape (N,) dtype=object with doc ids (strings)
      vecs: np.ndarray of shape (N, dim) float32 (L2-normalized recommended)
    """
    d = Path(root) / kind / model
    man = _load_manifest(d)
    parts = man.get("parts", [])
    all_ids: List[np.ndarray] = []
    all_vecs: List[np.ndarray] = []

    for entry in parts:
        part = entry["part"]
        npz = d / f"{part}.npz"
        pq = d / f"{part}.parquet"
        if npz.exists():
            ids, vecs = _load_part(npz)
        elif pq.exists():
            ids, vecs = _load_part(pq)
        else:
            raise FileNotFoundError(f"Part not found: {npz} or {pq}")
        all_ids.append(ids)
        all_vecs.append(vecs)

    if not all_ids:
        return np.empty((0,), dtype=object), np.empty((0, 0), dtype=np.float32)

    ids = np.concatenate(all_ids, axis=0)
    vecs = np.vstack(all_vecs).astype(np.float32, copy=False)
    return ids, vecs


def build_faiss_index(vecs: np.ndarray, metric: str = "ip") -> "faiss.Index":
    """
    Build a FAISS index in-memory.

    Parameters
    ----------
    vecs : (N, D) float32, ideally L2-normalized if using 'ip' (inner product).
    metric: 'ip' for inner product (cosine if normalized) or 'l2'.

    Returns
    -------
    faiss.Index
    """
    _ensure_faiss()
    if vecs.ndim != 2:
        raise ValueError("vecs must be 2D")
    d = vecs.shape[1]
    if metric == "ip":
        index = faiss.IndexFlatIP(d)
    elif metric == "l2":
        index = faiss.IndexFlatL2(d)
    else:
        raise ValueError("metric must be 'ip' or 'l2'")
    index.add(vecs)
    return index


def save_faiss_index(
    index: "faiss.Index", dir_out: str, *, kind: str, model: str, ids: np.ndarray
) -> Path:
    """
    Persist FAISS index and id mapping.

    Files
    -----
    {dir_out}/{kind}/{model}/index.faiss
    {dir_out}/{kind}/{model}/ids.npy
    """
    _ensure_faiss()
    d = Path(dir_out) / kind / model
    d.mkdir(parents=True, exist_ok=True)
    idx_path = d / "index.faiss"
    ids_path = d / "ids.npy"
    faiss.write_index(index, str(idx_path))
    np.save(ids_path, ids)
    return idx_path


def load_faiss_index(
    dir_out: str, *, kind: str, model: str
) -> Tuple["faiss.Index", np.ndarray]:
    """
    Load previously saved FAISS index and doc id mapping.
    """
    _ensure_faiss()
    d = Path(dir_out) / kind / model
    idx_path = d / "index.faiss"
    ids_path = d / "ids.npy"
    if not idx_path.exists() or not ids_path.exists():
        raise FileNotFoundError(f"Missing index or ids in {d}")
    index = faiss.read_index(str(idx_path))
    ids = np.load(ids_path, allow_pickle=True)
    return index, ids


def build_from_embeddings(
    emb_root: str,
    out_dir: str,
    *,
    kind: str,
    model: str,
    metric: str = "ip",
) -> Path:
    """
    Convenience helper to build & save an index from an embeddings directory.
    """
    ids, vecs = load_embeddings_dir(emb_root, kind, model)
    index = build_faiss_index(vecs, metric=metric)
    return save_faiss_index(index, out_dir, kind=kind, model=model, ids=ids)


if __name__ == "__main__":  # simple CLI usage
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--emb-root", default="data/embeddings")
    p.add_argument("--out-dir", default="data/index/faiss")
    p.add_argument(
        "--kind", required=True, choices=["text", "simulation", "timeseries"]
    )
    p.add_argument("--model", required=True)
    p.add_argument("--metric", default="ip", choices=["ip", "l2"])
    args = p.parse_args()

    path = build_from_embeddings(
        args.emb_root,
        args.out_dir,
        kind=args.kind,
        model=args.model,
        metric=args.metric,
    )
    print("Index saved at:", path)

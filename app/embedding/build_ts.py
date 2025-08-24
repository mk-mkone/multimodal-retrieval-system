import json
from pathlib import Path
from typing import Sequence

from app.embedding.ts_simple import SimpleTimeseriesEmbedding
from app.embedding.store import EmbeddingStore


def load_jsonl(path: str | Path) -> list[dict]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    items: list[dict] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        items.append(json.loads(line))
    return items


def run(jsonl_paths: Sequence[str], part_prefix: str = "part", fft_bins: int = 16):
    engine = SimpleTimeseriesEmbedding(fft_bins=fft_bins)
    store = EmbeddingStore()

    for i, jp in enumerate(jsonl_paths):
        items = load_jsonl(jp)
        if not items:
            print(f"[skip] no items in {jp}")
            continue

        ts_items = [d for d in items if d.get("kind") == "timeseries"]
        if not ts_items:
            print(f"[skip] no timeseries docs in {jp}")
            continue

        vecs = engine.embed_batch(ts_items)
        ids = [it["uid"] for it in ts_items]
        part = f"{part_prefix}-{i:03d}"
        out = store.save_part(
            kind="timeseries",
            model=engine.name,
            part=part,
            doc_ids=ids,
            vectors=vecs,
            fmt="auto",
        )
        print("Saved:", out)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m app.embedding.build_ts <standardized_ts.jsonl>")
        raise SystemExit(2)
    run(sys.argv[1:])

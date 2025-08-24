import json
from pathlib import Path
from typing import Sequence

from app.embedding.text_sbert import SbertTextEngine
from app.embedding.store import EmbeddingStore


def load_jsonl(path: str | Path) -> list[dict]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    items = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        items.append(json.loads(line))
    return items


def run(jsonl_paths: Sequence[str], part_prefix: str = "part"):
    engine = SbertTextEngine()
    store = EmbeddingStore()

    for i, jp in enumerate(jsonl_paths):
        items = load_jsonl(jp)
        if not items:
            print(f"[skip] no items in {jp}")
            continue
        vecs = engine.embed_batch(items)
        ids = [it["uid"] for it in items]
        part = f"{part_prefix}-{i:03d}"
        out = store.save_part(
            kind="text",
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
        print(
            "Usage: python -m app.embedding.build_text <standardized.jsonl>"
        )
        raise SystemExit(2)
    run(sys.argv[1:])

import json
import numpy as np


def test_embedding_store_saves_npz_and_manifest(tmp_path):
    from app.embedding.store import EmbeddingStore

    store = EmbeddingStore(root=tmp_path / "emb")

    ids = ["u1", "u2", "u3"]
    vecs = np.ones((3, 5), dtype=np.float32)

    out = store.save_part(
        kind="text",
        model="mini",
        part="part-000",
        doc_ids=ids,
        vectors=vecs,
        fmt="npz",
    )

    assert out.exists()
    assert out.suffix == ".npz"

    data = np.load(out, allow_pickle=True)
    assert list(data["ids"]) == ids
    assert data["vecs"].shape == (3, 5)

    man = out.parent / "manifest.json"
    assert man.exists()
    m = json.loads(man.read_text())
    assert m["kind"] == "text"
    assert m["model"] == "mini"
    assert m["parts"][0]["part"] == "part-000"
    assert m["parts"][0]["count"] == 3
    assert m["parts"][0]["dim"] == 5

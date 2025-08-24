import json
import numpy as np
import types


def test_build_text_runs_and_writes(tmp_path, mocker, capsys):
    fake = types.SimpleNamespace(
        get_sentence_embedding_dimension=lambda: 384,
        encode=lambda texts, **kw: np.ones((len(texts), 384), dtype=np.float32),
    )
    mocker.patch("sentence_transformers.SentenceTransformer", return_value=fake)

    jsonl = tmp_path / "std_text.jsonl"
    items = [
        {"uid": "t1", "kind": "text", "text": "hello world"},
        {"uid": "t2", "kind": "text", "title": "materials"},
    ]
    jsonl.write_text("\n".join(json.dumps(x) for x in items), encoding="utf-8")

    from app.embedding.store import EmbeddingStore

    out_dir = tmp_path / "emb_out"

    def _fake_dir(self, kind, model):
        p = out_dir / kind / model
        p.mkdir(parents=True, exist_ok=True)
        return p

    mocker.patch.object(EmbeddingStore, "_dir", _fake_dir)

    from app.embedding.build_text import run

    run([str(jsonl)], part_prefix="part")

    manifest = list(out_dir.rglob("manifest.json"))
    assert manifest, "manifest.json should exist"
    parts = list(out_dir.rglob("part-000.*"))
    assert parts, "embedding part should be written"

    out = capsys.readouterr().out
    assert "Saved:" in out

import json


def test_build_sim_runs_and_writes(tmp_path, mocker, capsys):
    jsonl = tmp_path / "std_sim.jsonl"
    items = [
        {"uid": "s1", "kind": "simulation", "material": {"elements": ["Si", "O", "O"]}},
        {"uid": "s2", "kind": "simulation", "material": {"elements": []}},
    ]
    jsonl.write_text("\n".join(json.dumps(x) for x in items), encoding="utf-8")

    from app.embedding.store import EmbeddingStore

    out_dir = tmp_path / "emb_out"

    def _fake_dir(self, kind, model):
        p = out_dir / kind / model
        p.mkdir(parents=True, exist_ok=True)
        return p

    mocker.patch.object(EmbeddingStore, "_dir", _fake_dir)

    from app.embedding.build_sim import run

    run([str(jsonl)], part_prefix="part")

    manifest = list(out_dir.rglob("manifest.json"))
    parts = list(out_dir.rglob("part-000.*"))
    assert manifest and parts

    out = capsys.readouterr().out
    assert "Saved:" in out

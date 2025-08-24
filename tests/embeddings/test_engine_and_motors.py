import numpy as np
import types


def test_text_engine_conforms_and_vectors(mocker):
    fake = types.SimpleNamespace(
        get_sentence_embedding_dimension=lambda: 384,
        encode=lambda texts, **kw: np.ones((len(texts), 384), dtype=np.float32),
    )
    mocker.patch("sentence_transformers.SentenceTransformer", return_value=fake)

    from app.embedding.text_sbert import SbertTextEngine

    eng = SbertTextEngine()
    docs = [{"uid": "1", "text": "hello"}, {"uid": "2", "title": "world"}]
    vecs = eng.embed_batch(docs)

    assert eng.modality == "text"
    assert isinstance(eng.dim, int) and eng.dim == 384
    assert vecs.shape == (2, 384)
    assert np.allclose(vecs[0], 1.0)


def test_sim_engine_conforms_and_vectors():
    from app.embedding.sim_simple import SimpleMaterialFingerprint

    eng = SimpleMaterialFingerprint()
    docs = [
        {"uid": "a", "kind": "simulation", "material": {"elements": ["Si", "O", "O"]}},
        {"uid": "b", "kind": "simulation", "material": {"elements": []}},
    ]
    vecs = eng.embed_batch(docs)

    assert eng.modality == "simulation"
    assert vecs.shape == (2, eng.dim)
    assert np.isclose(vecs[0].sum(), 1.0, atol=1e-6)
    assert float(vecs[1].sum()) == 0.0


def test_ts_engine_conforms_and_vectors():
    from app.embedding.ts_simple import SimpleTimeseriesEmbedding

    eng = SimpleTimeseriesEmbedding(fft_bins=8)
    docs = [
        {
            "uid": "t1",
            "kind": "timeseries",
            "values": [{"t": 0, "v": 1.0}, {"t": 1, "v": 2.0}, {"t": 2, "v": 1.5}],
        },
        {"uid": "t2", "kind": "timeseries", "values": []},
    ]
    vecs = eng.embed_batch(docs)

    assert eng.modality == "timeseries"
    assert vecs.shape == (2, 12)
    m, s, mn, mx = vecs[0, :4]
    assert mn <= m <= mx
    assert s >= 0.0
    assert float(vecs[1].sum()) == 0.0

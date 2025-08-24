import numpy as np
from app.embedding.sim_simple import SimpleMaterialFingerprint
from app.embedding.ts_simple import SimpleTimeseriesEmbedding


def test_simple_material_fingerprint_histogram_normalized():
    eng = SimpleMaterialFingerprint()
    docs = [
        {"uid": "a", "kind": "simulation", "material": {"elements": ["Si", "O", "O"]}},
        {
            "uid": "b",
            "kind": "simulation",
            "material": {"elements": []},
        },
    ]
    vecs = eng.embed_batch(docs)

    assert vecs.shape == (2, eng.dim)
    assert np.isclose(vecs[0].sum(), 1.0, atol=1e-6)
    assert float(vecs[1].sum()) == 0.0
    assert eng.modality == "simulation"


def test_simple_timeseries_embedding_shapes_and_values():
    eng = SimpleTimeseriesEmbedding(fft_bins=8)
    docs = [
        {
            "uid": "t1",
            "kind": "timeseries",
            "values": [{"t": 0, "v": 1.0}, {"t": 1, "v": 2.0}, {"t": 2, "v": 1.5}],
        },
        {"uid": "t2", "kind": "timeseries", "values": []},  # empty → zeros
    ]
    vecs = eng.embed_batch(docs)

    assert vecs.shape == (2, 4 + 8)
    m, s, mn, mx = vecs[0, :4]
    assert mn <= m <= mx
    assert s >= 0.0
    assert float(vecs[1].sum()) == 0.0
    assert eng.modality == "timeseries"

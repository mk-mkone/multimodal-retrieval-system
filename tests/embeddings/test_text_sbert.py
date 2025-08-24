import numpy as np
import types


def test_sbert_text_engine_mocked(mocker):
    fake = types.SimpleNamespace(
        get_sentence_embedding_dimension=lambda: 384,
        encode=lambda texts, **kw: np.ones((len(texts), 384), dtype=np.float32),
    )
    mocker.patch("sentence_transformers.SentenceTransformer", return_value=fake)

    from app.embedding.text_sbert import SbertTextEngine

    eng = SbertTextEngine(model_name="sentence-transformers/all-MiniLM-L6-v2")
    docs = [{"uid": "1", "text": "hello"}, {"uid": "2", "title": "world"}]
    vecs = eng.embed_batch(docs)

    assert vecs.shape == (2, 384)
    assert np.allclose(vecs[0], 1.0)
    assert eng.modality == "text"
    assert isinstance(eng.dim, int) and eng.dim == 384

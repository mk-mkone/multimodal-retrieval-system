from typing import Iterable, Optional
import numpy as np


class SbertTextEngine:
    """
    Sentence-Transformers text embedding engine (CPU by default).

    - Normalizes embeddings to unit-norm for cosine / inner-product search.
    - Extracts text from the standardized doc: prefer 'text', fallback to 'title'.
    """

    def __init__(
        self,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        device: Optional[str] = None,
    ):
        try:
            from sentence_transformers import SentenceTransformer
        except Exception as exc:
            raise RuntimeError(
                "sentence-transformers is required for SbertTextEngine"
            ) from exc

        self.model = SentenceTransformer(model_name, device=device or "cpu")
        self.name = model_name.split("/")[-1]
        self.modality = "text"

        _ = self.model.encode(
            ["warmup"], convert_to_numpy=True, normalize_embeddings=True
        )
        self.dim = int(self.model.get_sentence_embedding_dimension())

    def _texts(self, items: Iterable[dict]) -> list[str]:
        out: list[str] = []
        for d in items:
            txt = d.get("text") or d.get("title") or ""
            out.append(str(txt))
        return out

    def embed_batch(self, items: Iterable[dict]) -> np.ndarray:
        texts = self._texts(items)
        if not texts:
            return np.empty((0, self.dim), dtype=np.float32)
        vecs = self.model.encode(
            texts,
            convert_to_numpy=True,
            normalize_embeddings=True,
            batch_size=64,
            show_progress_bar=False,
        ).astype(np.float32)
        return vecs

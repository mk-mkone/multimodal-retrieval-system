from typing import Protocol, Iterable
import numpy as np


class EmbeddingEngine(Protocol):
    """
    Common interface for all embedding engines (text, simulation, timeseries).

    Attributes
    ----------
    name : str
        Stable identifier for the model/recipe (e.g. 'all-MiniLM-L6-v2').
    dim : int
        Embedding dimension produced by this engine.
    modality : str
        'text' | 'simulation' | 'timeseries'.

    Methods
    -------
    embed_batch(items):
        Convert an iterable of standardized docs (dict-like) into a (N, dim)
        float32 array of embeddings. The order of output vectors matches the
        input order.
    """

    name: str
    dim: int
    modality: str

    def embed_batch(self, items: Iterable[dict]) -> np.ndarray: ...

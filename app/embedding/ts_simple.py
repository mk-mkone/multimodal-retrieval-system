from typing import Iterable
import numpy as np


class SimpleTimeseriesEmbedding:
    """
    MVP timeseries embedding: [mean, std, min, max] + first N FFT magnitudes.

    Input expected format (standardized doc):
        {"values": [{"t": <float>, "v": <float>} ..]}
    """

    def __init__(self, fft_bins: int = 16):
        self.name = f"stats-fft{fft_bins}"
        self.modality = "timeseries"
        self.fft_bins = int(fft_bins)
        self.dim = 4 + self.fft_bins

    def embed_batch(self, items: Iterable[dict]) -> np.ndarray:
        vecs = []
        for d in items:
            vals = [p.get("v", 0.0) for p in d.get("values", [])]
            if not vals:
                vecs.append(np.zeros(self.dim, dtype=np.float32))
                continue

            arr = np.asarray(vals, dtype=np.float32)
            m, s, mn, mx = (
                float(arr.mean()),
                float(arr.std()),
                float(arr.min()),
                float(arr.max()),
            )

            n_fft = max(arr.shape[0], self.fft_bins * 2)
            mag = np.abs(np.fft.rfft(arr, n=n_fft))[1 : self.fft_bins + 1]
            if mag.shape[0] < self.fft_bins:
                mag = np.pad(mag, (0, self.fft_bins - mag.shape[0]))

            feat = np.concatenate([[m, s, mn, mx], mag.astype(np.float32)])
            vecs.append(feat.astype(np.float32))
        return np.stack(vecs) if vecs else np.empty((0, self.dim), dtype=np.float32)

import asyncio
from pathlib import Path
from typing import Literal

import pandas as pd

from app.ingestion.base import BaseIngestor
from app.preprocessing.pipeline import preprocess_timeseries
from typing import Optional
from app.core.registry import Registry


class TimeSeriesIngestor(BaseIngestor):
    """
    Ingests time series or spectroscopy data from local files (CSV, JSON, or NetCDF).
    The `fetch` and `fetch_async` methods only validate the file's existence and return its path as a string;
    actual parsing and reading of the file into a DataFrame is performed in the `parse` method.
    This class is intended to provide a unified interface for ingesting structured time series data from various file formats.
    """

    NAME = "timeseries"

    def __init__(self, *, registry: Optional[Registry] = None, **kw):
        super().__init__(registry=registry, **kw)
        self.standardize_fn = preprocess_timeseries

    def fetch(self, path: str, kind: Literal["csv", "json", "netcdf"] = "csv") -> str:
        """
        Validates that the specified file exists and returns its path as a string.
        The `kind` argument is informational; the actual file type is determined by extension in `parse`.
        """
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(path)
        return str(p)

    async def fetch_async(
        self,
        path: str,
        kind: Literal["csv", "json", "netcdf"] = "csv",
    ) -> str:
        """
        Asynchronously validates the existence of the file by delegating the operation to a thread,
        ensuring the event loop is not blocked.
        """
        return await asyncio.to_thread(self.fetch, path, kind)

    def parse(self, raw: str) -> pd.DataFrame:
        """
        Reads the file at the given path into a Pandas DataFrame based on its file extension.
        - For `.csv` files: uses `pandas.read_csv`.
        - For `.json` files: uses `pandas.read_json`, first attempting line-delimited JSON (`lines=True`),
          falling back to standard JSON arrays if necessary.
        - For `.nc` or `.netcdf` files: attempts to use the optional `xarray` dependency to open the dataset,
          flattening it into a DataFrame. Raises an error if `xarray` is not installed.
        Raises a ValueError for unsupported file extensions.
        """
        p = Path(raw)
        suffix = p.suffix.lower()

        if suffix == ".csv":
            df = pd.read_csv(p)

        elif suffix == ".json":
            try:
                df = pd.read_json(p, lines=True)
            except ValueError:
                df = pd.read_json(p)

        elif suffix in (".nc", ".netcdf"):
            try:
                import xarray as xr
            except ImportError as e:
                raise RuntimeError(
                    "xarray is required to read NetCDF files (.nc/.netcdf). "
                    "Add `xarray` to your dependencies."
                ) from e
            ds = xr.open_dataset(p)
            df = ds.to_dataframe().reset_index()

        else:
            raise ValueError(f"Unsupported file format: {p.suffix}")

        return df

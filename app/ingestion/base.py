import asyncio
import json
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Iterable, List, Optional

import pandas as pd
import requests

from app.core.logging_factory import LoggerFactory


class BaseIngestor(ABC):
    """
    BaseIngestor is an extensible template for ingestion pipelines.

    Responsibilities
    - Centralized logging and output directory management
    - HTTP helpers with retries (sync via `http_get_json`, async via `http_get_json_async`)
    - Canonical pipelines (`run` / `run_async`):
      fetch → save_raw → parse → save table → (optional) standardize → write_standardized

    Configuration
    - `NAME`: short identifier for the ingestor (used in logger name and output folder)
    - `out_dir`: base output directory (a subfolder named after `NAME` is created)
    - `standardize_fn`: callable used to transform parsed records into standardized pivot documents
    - `dump_standardized`: when True, writes a JSONL file with standardized docs
    - `standardized_filename`: output filename for standardized JSONL (default: `standardized.jsonl`)
    """

    NAME: str = "base"
    standardize_fn: Optional[Callable[[List[dict]], Iterable[Any]]] = None
    dump_standardized: bool = True
    standardized_filename: str = "standardized.jsonl"

    def __init__(self, out_dir: str = "data/raw"):
        """
        Initialize the ingestor with a specified output directory.

        Args:
            out_dir (str): Base directory where raw and parsed data will be saved.
        """
        self.logger = LoggerFactory.get_logger(f"ingestion.{self.NAME}")
        self.out_dir = Path(out_dir) / self.NAME
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def to_records(self, df: pd.DataFrame) -> List[dict]:
        """Convert parsed DataFrame to list of dict records (override if needed)."""
        return df.to_dict(orient="records")

    def standardize(self, records: List[dict]) -> Optional[Iterable[Any]]:
        """
        Hook to convert parsed records into standardized pivot documents.
        Default: if `standardize_fn` is set, delegate to it; otherwise return None.
        Subclasses may override this method to implement custom logic.
        """
        if self.standardize_fn:
            return list(self.standardize_fn(records))
        return None

    def write_standardized(self, docs: Iterable[Any]) -> Path:
        """
        Persist standardized documents as JSONL next to other outputs. Supports
        Pydantic models (model_dump_json / model_dump) or plain dicts.
        """
        out = self.out_dir / self.standardized_filename
        with out.open("w", encoding="utf-8") as f:
            for d in docs:
                if hasattr(d, "model_dump_json"):
                    f.write(d.model_dump_json() + "\n")
                elif hasattr(d, "model_dump"):
                    import json as _json

                    f.write(_json.dumps(d.model_dump()) + "\n")
                else:
                    import json as _json

                    f.write(_json.dumps(d) + "\n")
        self.logger.info("standardized_saved", extra={"path": str(out)})
        return out

    @abstractmethod
    def fetch(self, **kwargs) -> Any:
        """
        Retrieve raw data from an API or files. Returns raw object (dict/list/bytes...).

        Args:
            **kwargs: Additional parameters for fetching data.

        Returns:
            Any: Raw data retrieved.
        """
        ...

    @abstractmethod
    def parse(self, raw: Any) -> pd.DataFrame:
        """
        Transform raw data into a structured table (DataFrame) with a stable schema.

        Args:
            raw (Any): Raw data to parse.

        Returns:
            pd.DataFrame: Parsed structured data.
        """
        ...

    async def fetch_async(self, **kwargs) -> Any:
        """
        Asynchronous version of fetch(). By default, runs the synchronous version
        in a thread to avoid blocking the FastAPI event loop.
        Override in an ingestor if you use truly async I/O.

        Args:
            **kwargs: Additional parameters for fetching data.

        Returns:
            Any: Raw data retrieved asynchronously.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: self.fetch(**kwargs))

    async def run_async(self, **kwargs) -> pd.DataFrame:
        """
        Asynchronous pipeline: fetch_async -> save_raw -> parse -> save table.
        Same as run(), but non-blocking for the API.

        Args:
            **kwargs: Additional parameters for fetching/parsing data.

        Returns:
            pd.DataFrame: Parsed structured data.
        """
        self.logger.info(
            "start fetch", extra={"params": {k: str(v) for k, v in kwargs.items()}}
        )
        raw = await self.fetch_async(**kwargs)
        self.logger.info("fetched", extra={"info": self._brief(raw)})

        self.save_raw(raw)  # save raw trace
        df = await asyncio.to_thread(self.parse, raw)
        self.logger.info("parsed", extra={"rows": len(df)})

        await asyncio.to_thread(self._save_table, df)

        try:
            records = await asyncio.to_thread(self.to_records, df)
            docs = await asyncio.to_thread(self.standardize, records)
            if docs and self.dump_standardized:
                await asyncio.to_thread(self.write_standardized, docs)
        except Exception as e:
            self.logger.warning("standardize_failed", extra={"error": str(e)})

        return df

    def run(self, **kwargs) -> pd.DataFrame:
        """
        Synchronous pipeline: fetch -> save_raw -> parse -> save table.

        Args:
            **kwargs: Additional parameters for fetching/parsing data.

        Returns:
            pd.DataFrame: Parsed structured data.
        """
        self.logger.info(
            "start fetch", extra={"params": {k: str(v) for k, v in kwargs.items()}}
        )
        raw = self.fetch(**kwargs)
        self.logger.info("fetched", extra={"info": self._brief(raw)})

        self.save_raw(raw)  # save raw trace
        df = self.parse(raw)
        self.logger.info("parsed", extra={"rows": len(df)})

        self._save_table(df)

        try:
            records = self.to_records(df)
            docs = self.standardize(records)
            if docs and self.dump_standardized:
                self.write_standardized(docs)
        except Exception as e:
            self.logger.warning("standardize_failed", extra={"error": str(e)})

        return df

    def http_get_json(
        self,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        retries: int = 3,
        backoff: float = 1.0,
    ) -> dict:
        """
        Simple helper for HTTP GET returning JSON with retries.

        Args:
            url (str): URL to request.
            params (Optional[dict]): Query parameters.
            headers (Optional[dict]): HTTP headers.
            retries (int): Number of retry attempts.
            backoff (float): Backoff multiplier in seconds between retries.

        Returns:
            dict: JSON response parsed as a dictionary.

        Raises:
            HTTPError: If all retry attempts fail.
        """
        for attempt in range(1, retries + 1):
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            if resp.ok:
                return resp.json()
            self.logger.warning(
                "http_get_json failed",
                extra={"status": resp.status_code, "attempt": attempt, "url": url},
            )
            time.sleep(backoff * attempt)
        resp.raise_for_status()
        return {}

    async def http_get_json_async(
        self,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        retries: int = 3,
        backoff: float = 1.0,
    ) -> dict:
        """
        Asynchronous version of http_get_json() which runs the synchronous version in a thread.
        Replace with an httpx implementation if you want true async I/O.

        Args:
            url (str): URL to request.
            params (Optional[dict]): Query parameters.
            headers (Optional[dict]): HTTP headers.
            retries (int): Number of retry attempts.
            backoff (float): Backoff multiplier in seconds between retries.

        Returns:
            dict: JSON response parsed as a dictionary.
        """
        return await asyncio.to_thread(
            self.http_get_json, url, params, headers, retries, backoff
        )

    def save_raw(self, raw: Any, suffix: str = "json") -> Path:
        """
        Save raw data for audit and reproducibility.

        Args:
            raw (Any): Raw data to save.
            suffix (str): File extension suffix (e.g., 'json', 'txt').

        Returns:
            Path: Path to the saved raw data file.
        """
        ts = int(time.time() * 1000)
        path = self.out_dir / f"raw_{ts}.{suffix}"

        if isinstance(raw, (dict, list)):
            path.write_text(
                json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        elif isinstance(raw, (bytes, bytearray)):
            path.write_bytes(raw)
        else:
            path.write_text(str(raw), encoding="utf-8")

        self.logger.info("raw_saved", extra={"path": str(path)})
        return path

    def _save_table(self, df: pd.DataFrame, fmt: str = "parquet") -> Path:
        """
        Save parsed table data to file in parquet or CSV format.

        Args:
            df (pd.DataFrame): DataFrame to save.
            fmt (str): Format to save ('parquet' or 'csv').

        Returns:
            Path: Path to the saved table file.
        """
        ts = int(time.time() * 1000)
        path = self.out_dir / f"table_{ts}.{ 'parquet' if fmt=='parquet' else 'csv'}"
        if fmt == "parquet":
            df.to_parquet(path, index=False)
        else:
            df.to_csv(path, index=False)
        self.logger.info("table_saved", extra={"path": str(path), "rows": len(df)})
        return path

    @staticmethod
    def _brief(obj: Any) -> dict:
        """
        Lightweight summary for logging.

        Args:
            obj (Any): Object to summarize.

        Returns:
            dict: Summary dictionary with type and limited info.
        """
        if isinstance(obj, dict):
            return {"type": "dict", "keys": list(obj.keys())[:5]}
        if isinstance(obj, list):
            return {"type": "list", "len": len(obj)}
        return {"type": type(obj).__name__}

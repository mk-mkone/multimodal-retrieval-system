from typing import Any, Dict

import pandas as pd

from app.core.config import settings
from app.ingestion.base import BaseIngestor
from app.preprocessing.pipeline import preprocess_text
from typing import Optional
from app.core.registry import Registry


class EuropePMCIngestor(BaseIngestor):
    """
    Ingest publications and patents data from EuropePMC.
    This class provides methods to fetch and parse data from the EuropePMC RESTful API.
    Documentation: https://europepmc.org/RestfulWebService
    """

    NAME = "text"

    def __init__(self, *, registry: Optional[Registry] = None, **kw):
        super().__init__(registry=registry, **kw)
        self.standardize_fn = preprocess_text

    @staticmethod
    def _build_params(
        query: str,
        page: int = 1,
        page_size: int = 25,
        result_type: str = "lite",
    ) -> Dict[str, Any]:
        """
        Build the query parameters for the EuropePMC API request.

        Args:
            query (str): The search query string.
            page (int, optional): Page number for pagination. Defaults to 1.
            page_size (int, optional): Number of results per page. Defaults to 25.
            result_type (str, optional): Type of result detail ("lite" or "core"). Defaults to "lite".

        Returns:
            Dict[str, Any]: Dictionary of parameters to be sent with the API request.
        """
        return {
            "query": query,
            "format": "json",
            "page": page,
            "pageSize": page_size,
            "resultType": result_type,
        }

    def fetch(
        self,
        query: str,
        page: int = 1,
        page_size: int = 25,
        result_type: str = "lite",
    ) -> dict:
        """
        Retrieve data synchronously from the EuropePMC API.

        Args:
            query (str): The search query string.
            page (int, optional): Page number for pagination. Defaults to 1.
            page_size (int, optional): Number of results per page. Defaults to 25.
            result_type (str, optional): Type of result detail ("lite" or "core"). Defaults to "lite".

        Returns:
            dict: The JSON response from the EuropePMC API as a dictionary.
        """
        url = settings.EUROPEPMC_API_URL
        params = self._build_params(
            query=query, page=page, page_size=page_size, result_type=result_type
        )
        return self.http_get_json(url, params=params)

    def parse(self, raw: dict) -> pd.DataFrame:
        """
        Transform the raw EuropePMC API response into a Pandas DataFrame.

        Args:
            raw (dict): Raw JSON response from EuropePMC API.

        Returns:
            pd.DataFrame: DataFrame containing the parsed publication or patent records.
        """
        result_list = raw.get("resultList", {}).get("result", [])
        if not result_list:
            return pd.DataFrame()

        rows = []
        for r in result_list:
            rows.append(
                {
                    "id": r.get("id"),
                    "title": r.get("title"),
                    "source": r.get("source"),
                    "pub_year": r.get("pubYear"),
                    "doi": r.get("doi"),
                    "author_string": r.get("authorString"),
                    "journal_title": r.get("journalTitle"),
                    "text": r.get("title") or "",
                }
            )
        return pd.DataFrame(rows)

    async def fetch_async(
        self,
        query: str,
        page: int = 1,
        page_size: int = 25,
        result_type: str = "lite",
    ) -> dict:
        """
        Retrieve data asynchronously from the EuropePMC API.
        Useful for non-blocking operations in asynchronous frameworks like FastAPI.

        Args:
            query (str): The search query string.
            page (int, optional): Page number for pagination. Defaults to 1.
            page_size (int, optional): Number of results per page. Defaults to 25.
            result_type (str, optional): Type of result detail ("lite" or "core"). Defaults to "lite".

        Returns:
            dict: The JSON response from the EuropePMC API as a dictionary.
        """
        url = settings.EUROPEPMC_API_URL
        params = self._build_params(
            query=query, page=page, page_size=page_size, result_type=result_type
        )
        return await self.http_get_json_async(url, params=params)

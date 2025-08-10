from typing import Any, Dict, Optional

import pandas as pd

from app.core.config import settings
from app.ingestion.base import BaseIngestor


class MaterialsProjectIngestor(BaseIngestor):
    """
    Ingests materials metadata from the Materials Project v2 REST API.
    This class provides methods to fetch and parse materials simulation data,
    supporting both synchronous and asynchronous retrieval. The ingestor is designed
    to interact with the Materials Project API (https://materialsproject.org/api), which
    requires an API key for authentication. Data is typically retrieved in paginated form,
    and the ingestor can convert raw API responses into structured Pandas DataFrames for further analysis.
    """

    NAME = "simulation"

    def __init__(self, out_dir: str = "data/raw", api_key: Optional[str] = None):
        super().__init__(out_dir)
        self.api_key = api_key or settings.MATERIALS_PROJECT_API_KEY

    @staticmethod
    def _build_params(
        formula: str = "Si",
        per_page: int = 25,
        page: int = 1,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Build the parameters dictionary for a Materials Project v2 API request.

        Parameters:
            formula (str): Chemical formula to search for (e.g., "Si").
            per_page (int): Number of results per page (pagination).
            page (int): Page number to retrieve.
            extra (Optional[Dict[str, Any]]): Additional query parameters to include.

        Returns:
            Dict[str, Any]: Parameters dictionary suitable for passing to requests.
        """
        params: Dict[str, Any] = {
            "formula": formula,
            "_per_page": per_page,
            "_page": page,
        }
        if extra:
            params.update(extra)
        return params

    def fetch(self, formula: str = "Si", per_page: int = 25, page: int = 1) -> dict:
        """
        Retrieve data synchronously from the Materials Project v2 API.

        Parameters:
            formula (str): Chemical formula to search for (default "Si").
            per_page (int): Number of results per page (default 25).
            page (int): Page number to retrieve (default 1).

        Returns:
            dict: The raw JSON response from the Materials Project API.
        """
        url = settings.MP_API_URL
        headers = {"X-API-KEY": self.api_key} if self.api_key else {}
        params = self._build_params(formula=formula, per_page=per_page, page=page)
        data = self.http_get_json(url, params=params, headers=headers)
        return data

    async def fetch_async(
        self,
        formula: str = "Si",
        per_page: int = 25,
        page: int = 1,
    ) -> dict:
        """
        Asynchronously retrieve data from the Materials Project v2 API.

        This method is useful for non-blocking use cases, such as when integrating with FastAPI endpoints.

        Parameters:
            formula (str): Chemical formula to search for (default "Si").
            per_page (int): Number of results per page (default 25).
            page (int): Page number to retrieve (default 1).

        Returns:
            dict: The raw JSON response from the Materials Project API.
        """
        url = settings.MP_API_URL
        headers = {"X-API-KEY": self.api_key} if self.api_key else {}
        params = self._build_params(formula=formula, per_page=per_page, page=page)
        return await self.http_get_json_async(url, params=params, headers=headers)

    def parse(self, raw: dict) -> pd.DataFrame:
        """
        Convert a raw Materials Project API response to a Pandas DataFrame.

        Parameters:
            raw (dict): The raw response dictionary from the Materials Project API.
                         Expected to contain a "data" key with a list of material records.

        Returns:
            pd.DataFrame: A DataFrame with columns for material_id, formula, spacegroup, band_gap, and density.
                         If no data is present, returns an empty DataFrame.
        """
        data = raw.get("data", [])
        if not data:
            return pd.DataFrame()

        rows = []
        for item in data:
            rows.append(
                {
                    "material_id": item.get("material_id"),
                    "formula": item.get("formula_pretty"),
                    "spacegroup": (item.get("symmetry") or {}).get("symbol"),
                    "band_gap": (item.get("bandstructure") or {}).get("band_gap"),
                    "density": item.get("density"),
                }
            )
        return pd.DataFrame(rows)

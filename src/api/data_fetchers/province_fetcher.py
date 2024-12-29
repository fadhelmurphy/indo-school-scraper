import csv
import io
from typing import List, Dict, Any
from .base_fetcher import BaseFetcher
from utils.api_client import APIClient


class ProvinceFetcher(BaseFetcher):
    def __init__(self):
        self.api_client = APIClient()

    def fetch(self, data_id: str = None) -> List[Dict[str, Any]]:
        return self.api_client.get_provinces()

    def to_csv(self, data: List[Dict[str, Any]]) -> str:
        output = io.StringIO()
        writer = csv.DictWriter(
            output, fieldnames=["kode_wilayah", "nama"], extrasaction="ignore"
        )

        writer.writeheader()
        writer.writerows(data)

        return output.getvalue()

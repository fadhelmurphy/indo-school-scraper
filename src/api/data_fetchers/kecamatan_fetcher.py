import csv
import io
from typing import List, Dict, Any
from .base_fetcher import BaseFetcher
from utils.api_client import APIClient


class KecamatanFetcher(BaseFetcher):
    def __init__(self):
        self.api_client = APIClient()

    def fetch(self, kabupaten_id: str = None) -> List[Dict[str, Any]]:
        if not kabupaten_id:
            raise ValueError("kabupaten_id is required for fetching kecamatan data")

        return self.api_client.get_kecamatans(kabupaten_id)

    def to_csv(self, data: List[Dict[str, Any]]) -> str:
        output = io.StringIO()
        writer = csv.DictWriter(
            output, fieldnames=["kode_wilayah", "nama"], extrasaction="ignore"
        )

        writer.writeheader()
        writer.writerows(data)

        return output.getvalue()

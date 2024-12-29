import csv
import io
from typing import List, Dict, Any
from .base_fetcher import BaseFetcher
from utils.api_client import APIClient


class SchoolFetcher(BaseFetcher):
    def __init__(self):
        self.api_client = APIClient()

    def fetch(self, kecamatan_id: str = None) -> List[Dict[str, Any]]:
        if not kecamatan_id:
            raise ValueError("kecamatan_id is required for fetching school data")

        return self.api_client.get_schools(kecamatan_id)

    def to_csv(self, data: List[Dict[str, Any]]) -> str:
        output = io.StringIO()
        writer = csv.DictWriter(
            output, fieldnames=["npsn", "nama", "kode_wilayah"], extrasaction="ignore"
        )

        writer.writeheader()
        writer.writerows(data)

        return output.getvalue()

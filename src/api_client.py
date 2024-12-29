from typing import List, Optional
import urllib.request
import urllib.parse
import json
from models import Province, Kabupaten, Kecamatan, School
from rate_limiter import RateLimiter


class APIClient:
    def __init__(self):
        self.base_url = "https://api.data.belajar.id/data-portal-backend/v1"
        self.rate_limiter = RateLimiter()
        self.default_params = {"sortBy": "bentuk_pendidikan", "sortDir": "asc"}

    def _make_request(self, url: str, params: dict = None) -> Optional[dict]:
        self.rate_limiter.check_and_wait()

        if params:
            query_string = urllib.parse.urlencode({**self.default_params, **params})
            url = f"{url}?{query_string}"

        try:
            with urllib.request.urlopen(url) as response:
                return json.loads(response.read().decode())
        except urllib.error.URLError as e:
            print(f"Error fetching data: {e}")
            return None

    def get_provinces(self) -> List[Province]:
        url = f"{self.base_url}/master-data/satuan-pendidikan/statistics/0/descendants"
        response = self._make_request(url)

        if not response or "data" not in response:
            return []

        return [
            Province(code=item["kode_wilayah"], name=item["nama"])
            for item in response["data"]
        ]

    def get_kabupatens(self, province_code: str) -> List[Kabupaten]:
        url = f"{self.base_url}/master-data/satuan-pendidikan/statistics/{province_code}/descendants"
        response = self._make_request(url)

        if not response or "data" not in response:
            return []

        return [
            Kabupaten(
                code=item["kode_wilayah"],
                name=item["nama"],
                province_code=province_code,
            )
            for item in response["data"]
        ]

    def get_kecamatans(self, kabupaten_code: str) -> List[Kecamatan]:
        url = f"{self.base_url}/master-data/satuan-pendidikan/statistics/{kabupaten_code}/descendants"
        response = self._make_request(url)

        if not response or "data" not in response:
            return []

        return [
            Kecamatan(
                code=item["kode_wilayah"],
                name=item["nama"],
                kabupaten_code=kabupaten_code,
            )
            for item in response["data"]
        ]

    def get_schools(self, kecamatan_code: str) -> List[School]:
        url = f"{self.base_url}/master-data/ptk/search"
        schools = []
        offset = 0

        while True:
            params = {"kodeKecamatan": kecamatan_code, "limit": 20, "offset": offset}

            response = self._make_request(url, params)

            if not response or not response.get("data"):
                break

            schools.extend(
                [
                    School(
                        npsn=item.get("npsn", ""),
                        name=item.get("nama", ""),
                        kecamatan_code=kecamatan_code,
                    )
                    for item in response["data"]
                ]
            )

            offset += 20

        return schools

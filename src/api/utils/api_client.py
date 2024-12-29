import urllib.request
import urllib.parse
import json
import time
import logging
from typing import Dict, Any, List
from .retry_handler import RetryHandler
from .exceptions import RetryableError, MaxRetriesExceededError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class APIClient:
    def __init__(self):
        self.base_url = "https://api.data.belajar.id/data-portal-backend/v1"
        self.request_count = 0
        self.request_limit = 150
        self.cooldown_minutes = 3
        self.retry_handler = RetryHandler()

    def _make_request(self, url: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        def _do_request():
            self.request_count += 1

            if self.request_count >= self.request_limit:
                logger.info(
                    f"Reached request limit. Cooling down for {self.cooldown_minutes} minutes..."
                )
                time.sleep(self.cooldown_minutes * 60)
                self.request_count = 0

            if params:
                query_string = urllib.parse.urlencode(params)
                url_with_params = f"{url}?{query_string}"
            else:
                url_with_params = url

            try:
                request = urllib.request.Request(url_with_params)
                with urllib.request.urlopen(request) as response:
                    if response.status == 429:
                        raise RetryableError("Rate limit exceeded", status_code=429)
                    elif response.status != 200:
                        logger.error(f"Request failed with status {response.status}")
                        return {"data": []}

                    return json.loads(response.read().decode())
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    raise RetryableError("Rate limit exceeded", status_code=429)
                logger.error(f"HTTP Error: {e}")
                return {"data": []}
            except Exception as e:
                logger.error(f"Error making request: {e}")
                return {"data": []}

        try:
            return self.retry_handler.execute(_do_request)
        except MaxRetriesExceededError:
            logger.error("Max retries exceeded, returning empty data")
            return {"data": []}

    def get_provinces(self) -> List[Dict[str, Any]]:
        url = (
            f"{self.base_url}/master-data/satuan-pendidikan/statistics/360/descendants"
        )
        params = {"sortBy": "bentuk_pendidikan", "sortDir": "asc"}
        response = self._make_request(url, params)
        return response.get("data", [])

    def get_kabupatens(self, province_id: str) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/master-data/satuan-pendidikan/statistics/{province_id}/descendants"
        params = {"sortBy": "bentuk_pendidikan", "sortDir": "asc"}
        response = self._make_request(url, params)
        return response.get("data", [])

    def get_kecamatans(self, kabupaten_id: str) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/master-data/satuan-pendidikan/statistics/{kabupaten_id}/descendants"
        params = {"sortBy": "bentuk_pendidikan", "sortDir": "asc"}
        response = self._make_request(url, params)
        return response.get("data", [])

    def get_schools(self, kecamatan_id: str) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/master-data/ptk/search"
        params = {
            "kodeKecamatan": kecamatan_id,
            "sortBy": "bentuk_pendidikan",
            "sortDir": "asc",
        }
        response = self._make_request(url, params)
        return response.get("data", [])

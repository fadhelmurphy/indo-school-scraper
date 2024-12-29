from config import BASE_URL, DEFAULT_PARAMS
from utils import make_request


def fetch_provinces():
    """Fetch all provinces data"""
    url = f"{BASE_URL}/master-data/satuan-pendidikan/statistics/0/descendants"
    data = make_request(url, DEFAULT_PARAMS)
    return data.get("data", []) if data else []


def fetch_kabupatens(province_id):
    """Fetch all kabupatens for a given province"""
    url = (
        f"{BASE_URL}/master-data/satuan-pendidikan/statistics/{province_id}/descendants"
    )
    data = make_request(url, DEFAULT_PARAMS)
    return data.get("data", []) if data else []


def fetch_kecamatans(kabupaten_id):
    """Fetch all kecamatans for a given kabupaten"""
    url = f"{BASE_URL}/master-data/satuan-pendidikan/statistics/{kabupaten_id}/descendants"
    data = make_request(url, DEFAULT_PARAMS)
    return data.get("data", []) if data else []

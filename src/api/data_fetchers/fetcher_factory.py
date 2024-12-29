from .base_fetcher import BaseFetcher
from .province_fetcher import ProvinceFetcher
from .kabupaten_fetcher import KabupatenFetcher
from .kecamatan_fetcher import KecamatanFetcher
from .school_fetcher import SchoolFetcher


def create_fetcher(data_type: str) -> BaseFetcher:
    fetchers = {
        "provinsi": ProvinceFetcher,
        "kabupaten": KabupatenFetcher,
        "kecamatan": KecamatanFetcher,
        "school": SchoolFetcher,
    }

    fetcher_class = fetchers.get(data_type)
    if not fetcher_class:
        raise ValueError(f"Invalid data type: {data_type}")

    return fetcher_class()
